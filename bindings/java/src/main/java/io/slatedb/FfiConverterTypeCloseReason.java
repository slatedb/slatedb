package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeCloseReason implements FfiConverterRustBuffer<CloseReason> {
    INSTANCE;

    @Override
    public CloseReason read(ByteBuffer buf) {
        try {
            return CloseReason.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(CloseReason value) {
        return 4L;
    }

    @Override
    public void write(CloseReason value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




