package io.slatedb.jna.ffi;

import java.lang.ref.Cleaner;

class JavaLangRefCleanable implements UniffiCleaner.Cleanable {
  private final Cleaner.Cleanable cleanable;

  JavaLangRefCleanable(Cleaner.Cleanable cleanable) {
    this.cleanable = cleanable;
  }

  @Override
  public void clean() {
    cleanable.clean();
  }
}
