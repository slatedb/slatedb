package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class MergeOperatorImpl implements AutoCloseable, MergeOperator {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public MergeOperatorImpl(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any attempt to
   * actually use an object constructed this way will fail as there is no connected Rust object.
   */
  public MergeOperatorImpl(NoPointer noPointer) {
    this.pointer = null;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  @Override
  public synchronized void close() {
    // Only allow a single call to this method.
    // TODO(uniffi): maybe we should log a warning if called more than once?
    if (this.wasDestroyed.compareAndSet(false, true)) {
      // This decrement always matches the initial count of 1 given at creation time.
      if (this.callCounter.decrementAndGet() == 0L) {
        cleanable.clean();
      }
    }
  }

  public <R> R callWithPointer(Function<Pointer, R> block) {
    // Check and increment the call counter, to keep the object alive.
    // This needs a compare-and-set retry loop in case of concurrent updates.
    long c;
    do {
      c = this.callCounter.get();
      if (c == 0L) {
        throw new IllegalStateException("MergeOperatorImpl object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("MergeOperatorImpl call counter would overflow");
      }
    } while (!this.callCounter.compareAndSet(c, c + 1L));
    // Now we can safely do the method call without the pointer being freed concurrently.
    try {
      return block.apply(this.uniffiClonePointer());
    } finally {
      // This decrement always matches the increment we performed above.
      if (this.callCounter.decrementAndGet() == 0L) {
        cleanable.clean();
      }
    }
  }

  public void callWithPointer(Consumer<Pointer> block) {
    callWithPointer(
        (Pointer p) -> {
          block.accept(p);
          return (Void) null;
        });
  }

  private class UniffiCleanAction implements Runnable {
    private final Pointer pointer;

    public UniffiCleanAction(Pointer pointer) {
      this.pointer = pointer;
    }

    @Override
    public void run() {
      if (pointer != null) {
        UniffiHelpers.uniffiRustCall(
            status -> {
              UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_free_mergeoperator(pointer, status);
              return null;
            });
      }
    }
  }

  Pointer uniffiClonePointer() {
    return UniffiHelpers.uniffiRustCall(
        status -> {
          if (pointer == null) {
            throw new NullPointerException();
          }
          return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_clone_mergeoperator(pointer, status);
        });
  }

  @Override
  public byte[] merge(byte[] key, byte[] existingValue, byte[] operand)
      throws MergeOperatorCallbackException {
    try {
      return FfiConverterByteArray.INSTANCE.lift(
          callWithPointer(
              it -> {
                try {

                  return UniffiHelpers.uniffiRustCallWithError(
                      new MergeOperatorCallbackExceptionErrorHandler(),
                      _status -> {
                        return UniffiLib.INSTANCE
                            .uniffi_slatedb_uniffi_fn_method_mergeoperator_merge(
                                it,
                                FfiConverterByteArray.INSTANCE.lower(key),
                                FfiConverterOptionalByteArray.INSTANCE.lower(existingValue),
                                FfiConverterByteArray.INSTANCE.lower(operand),
                                _status);
                      });

                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }));
    } catch (RuntimeException _e) {

      if (MergeOperatorCallbackException.class.isInstance(_e.getCause())) {
        throw (MergeOperatorCallbackException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }
}
