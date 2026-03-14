package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiDbIterator implements FfiConverter<FfiDbIterator, Pointer> {
  INSTANCE;

  @Override
  public Pointer lower(FfiDbIterator value) {
    return value.uniffiClonePointer();
  }

  @Override
  public FfiDbIterator lift(Pointer value) {
    return new FfiDbIterator(value);
  }

  @Override
  public FfiDbIterator read(ByteBuffer buf) {
    // The Rust code always writes pointers as 8 bytes, and will
    // fail to compile if they don't fit.
    return lift(new Pointer(buf.getLong()));
  }

  @Override
  public long allocationSize(FfiDbIterator value) {
    return 8L;
  }

  @Override
  public void write(FfiDbIterator value, ByteBuffer buf) {
    // The Rust code always expects pointers written as 8 bytes,
    // and will fail to compile if they don't fit.
    buf.putLong(Pointer.nativeValue(lower(value)));
  }
}

// This template implements a class for working with a Rust struct via a Pointer/Arc<T>
// to the live Rust struct on the other side of the FFI.
//
// Each instance implements core operations for working with the Rust `Arc<T>` and the
// Kotlin Pointer to work with the live Rust struct on the other side of the FFI.
//
// There's some subtlety here, because we have to be careful not to operate on a Rust
// struct after it has been dropped, and because we must expose a public API for freeing
// the Java wrapper object in lieu of reliable finalizers. The core requirements are:
//
//   * Each instance holds an opaque pointer to the underlying Rust struct.
//     Method calls need to read this pointer from the object's state and pass it in to
//     the Rust FFI.
//
//   * When an instance is no longer needed, its pointer should be passed to a
//     special destructor function provided by the Rust FFI, which will drop the
//     underlying Rust struct.
//
//   * Given an instance, calling code is expected to call the special
//     `close` method in order to free it after use, either by calling it explicitly
//     or by using a higher-level helper like `try-with-resources`. Failing to do so risks
//     leaking the underlying Rust struct.
//
//   * We can't assume that calling code will do the right thing, and must be prepared
//     to handle Java method calls executing concurrently with or even after a call to
//     `close`, and to handle multiple (possibly concurrent!) calls to `close`.
//
//   * We must never allow Rust code to operate on the underlying Rust struct after
//     the destructor has been called, and must never call the destructor more than once.
//     Doing so may trigger memory unsafety.
//
//   * To mitigate many of the risks of leaking memory and use-after-free unsafety, a `Cleaner`
//     is implemented to call the destructor when the Java object becomes unreachable.
//     This is done in a background thread. This is not a panacea, and client code should be aware
// that
//      1. the thread may starve if some there are objects that have poorly performing
//     `drop` methods or do significant work in their `drop` methods.
//      2. the thread is shared across the whole library. This can be tuned by using
// `android_cleaner = true`,
//         or `android = true` in the [`java` section of the `uniffi.toml` file, like the Kotlin
// one](https://mozilla.github.io/uniffi-rs/kotlin/configuration.html).
//
// If we try to implement this with mutual exclusion on access to the pointer, there is the
// possibility of a race between a method call and a concurrent call to `close`:
//
//    * Thread A starts a method call, reads the value of the pointer, but is interrupted
//      before it can pass the pointer over the FFI to Rust.
//    * Thread B calls `close` and frees the underlying Rust struct.
//    * Thread A resumes, passing the already-read pointer value to Rust and triggering
//      a use-after-free.
//
// One possible solution would be to use a `ReadWriteLock`, with each method call taking
// a read lock (and thus allowed to run concurrently) and the special `close` method
// taking a write lock (and thus blocking on live method calls). However, we aim not to
// generate methods with any hidden blocking semantics, and a `close` method that might
// block if called incorrectly seems to meet that bar.
//
// So, we achieve our goals by giving each instance an associated `AtomicLong` counter to track
// the number of in-flight method calls, and an `AtomicBoolean` flag to indicate whether `close`
// has been called. These are updated according to the following rules:
//
//    * The initial value of the counter is 1, indicating a live object with no in-flight calls.
//      The initial value for the flag is false.
//
//    * At the start of each method call, we atomically check the counter.
//      If it is 0 then the underlying Rust struct has already been destroyed and the call is
// aborted.
//      If it is nonzero them we atomically increment it by 1 and proceed with the method call.
//
//    * At the end of each method call, we atomically decrement and check the counter.
//      If it has reached zero then we destroy the underlying Rust struct.
//
//    * When `close` is called, we atomically flip the flag from false to true.
//      If the flag was already true we silently fail.
//      Otherwise we atomically decrement and check the counter.
//      If it has reached zero then we destroy the underlying Rust struct.
//
// Astute readers may observe that this all sounds very similar to the way that Rust's `Arc<T>`
// works,
// and indeed it is, with the addition of a flag to guard against multiple calls to `close`.
//
// The overall effect is that the underlying Rust struct is destroyed only when `close` has been
// called *and* all in-flight method calls have completed, avoiding violating any of the
// expectations
// of the underlying Rust code.
//
// This makes a cleaner a better alternative to _not_ calling `close()` as
// and when the object is finished with, but the abstraction is not perfect: if the Rust object's
// `drop`
// method is slow, and/or there are many objects to cleanup, and it's on a low end Android device,
// then the cleaner
// thread may be starved, and the app will leak memory.
//
// In this case, `close`ing manually may be a better solution.
//
// The cleaner can live side by side with the manual calling of `close`. In the order of
// responsiveness, uniffi objects
// with Rust peers are reclaimed:
//
// 1. By calling the `close` method of the object, which calls `rustObject.free()`. If that doesn't
// happen:
// 2. When the object becomes unreachable, AND the Cleaner thread gets to call `rustObject.free()`.
// If the thread is starved then:
// 3. The memory is reclaimed when the process terminates.
//
// [1]
// https://stackoverflow.com/questions/24376768/can-java-finalize-an-object-when-it-is-still-in-scope/24380219
//
