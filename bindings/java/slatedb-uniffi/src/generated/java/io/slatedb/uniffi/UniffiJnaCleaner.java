package io.slatedb.uniffi;


import com.sun.jna.internal.Cleaner;

// The fallback Jna cleaner, which is available for both Android, and the JVM.
class UniffiJnaCleaner implements UniffiCleaner {
    private final Cleaner cleaner = Cleaner.getCleaner();

    @Override
    public UniffiCleaner.Cleanable register(Object value, Runnable cleanUpTask) {
        return new UniffiJnaCleanable(cleaner.register(value, cleanUpTask));
    }
}

