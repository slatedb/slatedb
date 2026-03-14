package io.slatedb.jna.ffi;

public class FfiMergeOperatorCallbackExceptionErrorHandler
    implements UniffiRustCallStatusErrorHandler<FfiMergeOperatorCallbackException> {
  @Override
  public FfiMergeOperatorCallbackException lift(RustBuffer.ByValue errorBuf) {
    return FfiConverterTypeFfiMergeOperatorCallbackError.INSTANCE.lift(errorBuf);
  }
}
