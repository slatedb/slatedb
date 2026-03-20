package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeMergeOperatorCallbackError implements FfiConverterRustBuffer<MergeOperatorCallbackException> {
    INSTANCE;

    @Override
    public MergeOperatorCallbackException read(ByteBuffer buf) {

        return switch(buf.getInt()) {
            case 1 -> new MergeOperatorCallbackException.Failed(
                FfiConverterString.INSTANCE.read(buf)
                );
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }

    @Override
    public long allocationSize(MergeOperatorCallbackException value) {
        return switch(value) {
            case MergeOperatorCallbackException.Failed x -> (
                // Add the size for the Int that specifies the variant plus the size needed for all fields
                4L
                + FfiConverterString.INSTANCE.allocationSize(x.message)
            );
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }

    @Override
    public void write(MergeOperatorCallbackException value, ByteBuffer buf) {
        switch(value) {
            case MergeOperatorCallbackException.Failed x -> {
                buf.putInt(1);
                FfiConverterString.INSTANCE.write(x.message, buf);
            }
            default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
        };
    }
}


