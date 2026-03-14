package io.slatedb.jna.ffi;

import com.sun.jna.Structure;

@Structure.FieldOrder({"returnValue", "callStatus"})
public class UniffiForeignFutureStructRustBuffer extends Structure {
  public RustBuffer.ByValue returnValue = new RustBuffer.ByValue();
  public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

  // no-arg constructor required so JNA can instantiate and reflect
  public UniffiForeignFutureStructRustBuffer() {
    super();
  }

  public UniffiForeignFutureStructRustBuffer(
      RustBuffer.ByValue returnValue, UniffiRustCallStatus.ByValue callStatus) {
    this.returnValue = returnValue;
    this.callStatus = callStatus;
  }

  public static class UniffiByValue extends UniffiForeignFutureStructRustBuffer
      implements Structure.ByValue {
    public UniffiByValue(RustBuffer.ByValue returnValue, UniffiRustCallStatus.ByValue callStatus) {
      super(returnValue, callStatus);
    }
  }

  void uniffiSetValue(UniffiForeignFutureStructRustBuffer other) {
    returnValue = other.returnValue;
    callStatus = other.callStatus;
  }
}
