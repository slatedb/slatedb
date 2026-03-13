package io.slatedb;


import java.nio.ByteBuffer;

public abstract class FfiConverterCallbackInterface<CallbackInterface extends Object> implements FfiConverter<CallbackInterface, Long> {
    // Magic number for the Rust proxy to call using the same mechanism as every other method,
    // to free the callback once it's dropped by Rust.
    static final int IDX_CALLBACK_FREE = 0;
    // Callback return codes
    static final int UNIFFI_CALLBACK_SUCCESS = 0;
    static final int UNIFFI_CALLBACK_ERROR = 1;
    static final int UNIFFI_CALLBACK_UNEXPECTED_ERROR = 2;

    public final UniffiHandleMap<CallbackInterface> handleMap = new UniffiHandleMap<>();

    void drop(long handle) {
        handleMap.remove(handle);
    }

    @Override
    public CallbackInterface lift(Long value) {
        return handleMap.get(value);
    }

    @Override
    public CallbackInterface read(ByteBuffer buf) {
      return lift(buf.getLong());
    }

    @Override
    public Long lower(CallbackInterface value) {
      return handleMap.insert(value);
    }

    @Override
    public long allocationSize(CallbackInterface value) {
      return 8L;
    }

    @Override
    public void write(CallbackInterface value, ByteBuffer buf) {
        buf.putLong(lower(value));
    }
}

