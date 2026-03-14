package io.slatedb.jna.ffi;

public interface UniffiRustCallStatusErrorHandler<E extends Exception> {
  E lift(RustBuffer.ByValue errorBuf);
}
