package io.slatedb.jna.ffi;

import com.sun.jna.Structure;

@Structure.FieldOrder({"merge", "uniffiFree"})
public class UniffiVTableCallbackInterfaceFfiMergeOperator extends Structure {
  public UniffiCallbackInterfaceFfiMergeOperatorMethod0 merge = null;
  public UniffiCallbackInterfaceFree uniffiFree = null;

  // no-arg constructor required so JNA can instantiate and reflect
  public UniffiVTableCallbackInterfaceFfiMergeOperator() {
    super();
  }

  public UniffiVTableCallbackInterfaceFfiMergeOperator(
      UniffiCallbackInterfaceFfiMergeOperatorMethod0 merge,
      UniffiCallbackInterfaceFree uniffiFree) {
    this.merge = merge;
    this.uniffiFree = uniffiFree;
  }

  public static class UniffiByValue extends UniffiVTableCallbackInterfaceFfiMergeOperator
      implements Structure.ByValue {
    public UniffiByValue(
        UniffiCallbackInterfaceFfiMergeOperatorMethod0 merge,
        UniffiCallbackInterfaceFree uniffiFree) {
      super(merge, uniffiFree);
    }
  }

  void uniffiSetValue(UniffiVTableCallbackInterfaceFfiMergeOperator other) {
    merge = other.merge;
    uniffiFree = other.uniffiFree;
  }
}
