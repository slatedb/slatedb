package io.slatedb.uniffi;


public class ErrorErrorHandler implements UniffiRustCallStatusErrorHandler<Error> {
  @Override
  public Error lift(RustBuffer.ByValue errorBuf){
     return FfiConverterTypeError.INSTANCE.lift(errorBuf);
  }
}

