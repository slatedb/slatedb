package io.slatedb;


public class MergeOperatorCallbackExceptionErrorHandler implements UniffiRustCallStatusErrorHandler<MergeOperatorCallbackException> {
  @Override
  public MergeOperatorCallbackException lift(RustBuffer.ByValue errorBuf){
     return FfiConverterTypeMergeOperatorCallbackError.INSTANCE.lift(errorBuf);
  }
}

