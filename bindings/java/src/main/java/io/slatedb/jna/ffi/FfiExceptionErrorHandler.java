package io.slatedb.jna.ffi;

public class FfiExceptionErrorHandler implements UniffiRustCallStatusErrorHandler<FfiException> {
  @Override
  public FfiException lift(RustBuffer.ByValue errorBuf) {
    return FfiConverterTypeFfiError.INSTANCE.lift(errorBuf);
  }
}
