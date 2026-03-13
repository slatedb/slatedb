package io.slatedb;


public class SlatedbExceptionErrorHandler implements UniffiRustCallStatusErrorHandler<SlatedbException> {
  @Override
  public SlatedbException lift(RustBuffer.ByValue errorBuf){
     return FfiConverterTypeSlatedbError.INSTANCE.lift(errorBuf);
  }
}

