package io.slatedb.jna.ffi;

// The cleaner interface for Object finalization code to run.
// This is the entry point to any implementation that we're using.
//
// The cleaner registers objects and returns cleanables, so now we are
// defining a `UniffiCleaner` with a `UniffiClenaer.Cleanable` to abstract the
// different implmentations available at compile time.
interface UniffiCleaner {
  interface Cleanable {
    void clean();
  }

  UniffiCleaner.Cleanable register(Object value, Runnable cleanUpTask);

  public static UniffiCleaner create() {

    try {
      // For safety's sake: if the library hasn't been run in android_cleaner = true
      // mode, but is being run on Android, then we still need to think about
      // Android API versions.
      // So we check if java.lang.ref.Cleaner is there, and use that…
      Class.forName("java.lang.ref.Cleaner");
      return new JavaLangRefCleaner();
    } catch (ClassNotFoundException e) {
      // … otherwise, fallback to the JNA cleaner.
      return new UniffiJnaCleaner();
    }
  }
}
