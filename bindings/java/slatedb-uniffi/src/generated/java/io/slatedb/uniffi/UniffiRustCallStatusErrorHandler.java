package io.slatedb.uniffi;


public interface UniffiRustCallStatusErrorHandler<E extends Exception> {
    E lift(RustBuffer.ByValue errorBuf);
}

