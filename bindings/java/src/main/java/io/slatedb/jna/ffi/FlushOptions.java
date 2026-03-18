package io.slatedb.jna.ffi;

import java.util.Objects;

public class FlushOptions {
  private FlushType flushType;

  public FlushOptions(FlushType flushType) {

    this.flushType = flushType;
  }

  public FlushType flushType() {
    return this.flushType;
  }

  public void setFlushType(FlushType flushType) {
    this.flushType = flushType;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FlushOptions) {
      FlushOptions t = (FlushOptions) other;
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
