package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiFlushOptions {
  private FfiFlushType flushType;

  public FfiFlushOptions(FfiFlushType flushType) {

    this.flushType = flushType;
  }

  public FfiFlushType flushType() {
    return this.flushType;
  }

  public void setFlushType(FfiFlushType flushType) {
    this.flushType = flushType;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiFlushOptions) {
      FfiFlushOptions t = (FfiFlushOptions) other;
      return (Objects.equals(flushType, t.flushType));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(flushType);
  }
}
