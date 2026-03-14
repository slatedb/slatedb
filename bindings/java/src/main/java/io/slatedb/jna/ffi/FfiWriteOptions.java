package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiWriteOptions {
  private Boolean awaitDurable;

  public FfiWriteOptions(Boolean awaitDurable) {

    this.awaitDurable = awaitDurable;
  }

  public Boolean awaitDurable() {
    return this.awaitDurable;
  }

  public void setAwaitDurable(Boolean awaitDurable) {
    this.awaitDurable = awaitDurable;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiWriteOptions) {
      FfiWriteOptions t = (FfiWriteOptions) other;
      return (Objects.equals(awaitDurable, t.awaitDurable));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(awaitDurable);
  }
}
