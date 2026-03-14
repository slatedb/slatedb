package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiPutOptions {
  private FfiTtl ttl;

  public FfiPutOptions(FfiTtl ttl) {

    this.ttl = ttl;
  }

  public FfiTtl ttl() {
    return this.ttl;
  }

  public void setTtl(FfiTtl ttl) {
    this.ttl = ttl;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiPutOptions) {
      FfiPutOptions t = (FfiPutOptions) other;
      return (Objects.equals(ttl, t.ttl));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(ttl);
  }
}
