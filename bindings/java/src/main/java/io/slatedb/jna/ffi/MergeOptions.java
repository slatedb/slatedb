package io.slatedb.jna.ffi;

import java.util.Objects;

public class MergeOptions {
  private Ttl ttl;

  public MergeOptions(Ttl ttl) {

    this.ttl = ttl;
  }

  public Ttl ttl() {
    return this.ttl;
  }

  public void setTtl(Ttl ttl) {
    this.ttl = ttl;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MergeOptions) {
      MergeOptions t = (MergeOptions) other;
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
