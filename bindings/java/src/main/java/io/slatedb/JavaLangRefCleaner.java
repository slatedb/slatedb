package io.slatedb;


import java.lang.ref.Cleaner;

class JavaLangRefCleaner implements UniffiCleaner {
    private final Cleaner cleaner;

    JavaLangRefCleaner() {
      this.cleaner = Cleaner.create();
    }

    @Override
    public UniffiCleaner.Cleanable register(Object value, Runnable cleanUpTask) {
        return new JavaLangRefCleanable(cleaner.register(value, cleanUpTask));
    }
}

