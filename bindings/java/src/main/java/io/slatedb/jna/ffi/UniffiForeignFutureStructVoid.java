package io.slatedb.jna.ffi;

import com.sun.jna.Structure;

@Structure.FieldOrder({"callStatus"})
public class UniffiForeignFutureStructVoid extends Structure {
  public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

  // no-arg constructor required so JNA can instantiate and reflect
  public UniffiForeignFutureStructVoid() {
    super();
  }

  public UniffiForeignFutureStructVoid(UniffiRustCallStatus.ByValue callStatus) {
    this.callStatus = callStatus;
  }

  public static class UniffiByValue extends UniffiForeignFutureStructVoid
      implements Structure.ByValue {
    public UniffiByValue(UniffiRustCallStatus.ByValue callStatus) {
      super(callStatus);
    }
  }

  void uniffiSetValue(UniffiForeignFutureStructVoid other) {
    callStatus = other.callStatus;
  }
}
