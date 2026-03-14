package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiMergeOptions {
  private FfiTtl ttl;

  public FfiMergeOptions(FfiTtl ttl) {

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
    if (other instanceof FfiMergeOptions) {
      FfiMergeOptions t = (FfiMergeOptions) other;
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
