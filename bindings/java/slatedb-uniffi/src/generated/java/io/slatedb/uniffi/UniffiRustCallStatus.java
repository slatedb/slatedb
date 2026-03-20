package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "code", "error_buf" })
public class UniffiRustCallStatus extends Structure {
    public byte code;
    public RustBuffer.ByValue error_buf;

    public static class ByValue extends UniffiRustCallStatus implements Structure.ByValue {}

    public boolean isSuccess() {
        return code == UNIFFI_CALL_SUCCESS;
    }

    public boolean isError() {
        return code == UNIFFI_CALL_ERROR;
    }

    public boolean isPanic() {
        return code == UNIFFI_CALL_UNEXPECTED_ERROR;
    }

    public void setCode(byte code) {
      this.code = code;
    }

    public void setErrorBuf(RustBuffer.ByValue errorBuf) {
      this.error_buf = errorBuf;
    }

    public static UniffiRustCallStatus.ByValue create(byte code, RustBuffer.ByValue errorBuf) {
        UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();
        callStatus.code = code;
        callStatus.error_buf = errorBuf;
        return callStatus;
    }

    public static final byte UNIFFI_CALL_SUCCESS = 0;
    public static final byte UNIFFI_CALL_ERROR = 1;
    public static final byte UNIFFI_CALL_UNEXPECTED_ERROR = 2;
}

