package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeFlushType implements FfiConverterRustBuffer<FlushType> {
    INSTANCE;

    @Override
    public FlushType read(ByteBuffer buf) {
        try {
            return FlushType.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(FlushType value) {
        return 4L;
    }

    @Override
    public void write(FlushType value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




