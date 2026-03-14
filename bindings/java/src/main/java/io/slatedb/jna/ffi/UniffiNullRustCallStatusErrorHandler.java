package io.slatedb.jna.ffi;

// UniffiRustCallStatusErrorHandler implementation for times when we don't expect a CALL_ERROR
class UniffiNullRustCallStatusErrorHandler
    implements UniffiRustCallStatusErrorHandler<InternalException> {
  @Override
  public InternalException lift(RustBuffer.ByValue errorBuf) {
    RustBuffer.free(errorBuf);
    return new InternalException("Unexpected CALL_ERROR");
  }
}
