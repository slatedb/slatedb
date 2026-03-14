package io.slatedb.jna.ffi;

import com.sun.jna.Structure;

@Structure.FieldOrder({"log", "uniffiFree"})
public class UniffiVTableCallbackInterfaceFfiLogCallback extends Structure {
  public UniffiCallbackInterfaceFfiLogCallbackMethod0 log = null;
  public UniffiCallbackInterfaceFree uniffiFree = null;

  // no-arg constructor required so JNA can instantiate and reflect
  public UniffiVTableCallbackInterfaceFfiLogCallback() {
    super();
  }

  public UniffiVTableCallbackInterfaceFfiLogCallback(
      UniffiCallbackInterfaceFfiLogCallbackMethod0 log, UniffiCallbackInterfaceFree uniffiFree) {
    this.log = log;
    this.uniffiFree = uniffiFree;
  }

  public static class UniffiByValue extends UniffiVTableCallbackInterfaceFfiLogCallback
      implements Structure.ByValue {
    public UniffiByValue(
        UniffiCallbackInterfaceFfiLogCallbackMethod0 log, UniffiCallbackInterfaceFree uniffiFree) {
      super(log, uniffiFree);
    }
  }

  void uniffiSetValue(UniffiVTableCallbackInterfaceFfiLogCallback other) {
    log = other.log;
    uniffiFree = other.uniffiFree;
  }
}
