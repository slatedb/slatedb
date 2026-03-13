package io.slatedb;


public interface UniffiRustCallStatusErrorHandler<E extends Exception> {
    E lift(RustBuffer.ByValue errorBuf);
}

