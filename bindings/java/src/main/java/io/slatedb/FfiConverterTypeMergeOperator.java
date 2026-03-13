package io.slatedb;


// The ffiConverter which transforms the Callbacks in to handles to pass to Rust.
public final class FfiConverterTypeMergeOperator extends FfiConverterCallbackInterface<MergeOperator> {
  static final FfiConverterTypeMergeOperator INSTANCE = new FfiConverterTypeMergeOperator();

  private FfiConverterTypeMergeOperator() {}
}




