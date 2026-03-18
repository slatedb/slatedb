package io.slatedb.jna.ffi;

import java.util.Objects;

public class WriteOptions {
  private Boolean awaitDurable;

  public WriteOptions(Boolean awaitDurable) {

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
    if (other instanceof WriteOptions) {
      WriteOptions t = (WriteOptions) other;
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
