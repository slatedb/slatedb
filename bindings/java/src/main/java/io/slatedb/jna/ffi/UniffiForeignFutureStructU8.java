package io.slatedb.jna.ffi;

import com.sun.jna.Structure;

@Structure.FieldOrder({"returnValue", "callStatus"})
public class UniffiForeignFutureStructU8 extends Structure {
  public byte returnValue = (byte) 0;
  public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

  // no-arg constructor required so JNA can instantiate and reflect
  public UniffiForeignFutureStructU8() {
    super();
  }

  public UniffiForeignFutureStructU8(byte returnValue, UniffiRustCallStatus.ByValue callStatus) {
    this.returnValue = returnValue;
    this.callStatus = callStatus;
  }

  public static class UniffiByValue extends UniffiForeignFutureStructU8
      implements Structure.ByValue {
    public UniffiByValue(byte returnValue, UniffiRustCallStatus.ByValue callStatus) {
      super(returnValue, callStatus);
    }
  }

  void uniffiSetValue(UniffiForeignFutureStructU8 other) {
    returnValue = other.returnValue;
    callStatus = other.callStatus;
  }
}
