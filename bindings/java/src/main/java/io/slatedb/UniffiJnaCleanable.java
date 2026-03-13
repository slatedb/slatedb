package io.slatedb;


import com.sun.jna.internal.Cleaner;

class UniffiJnaCleanable implements UniffiCleaner.Cleanable {
    private final Cleaner.Cleanable cleanable;

    public UniffiJnaCleanable(Cleaner.Cleanable cleanable) {
        this.cleanable = cleanable;
    }

    @Override
    public void clean() {
        cleanable.clean();
    }
}

// We decide at uniffi binding generation time whether we were
// using Android or not.
// There are further runtime checks to chose the correct implementation
// of the cleaner.
