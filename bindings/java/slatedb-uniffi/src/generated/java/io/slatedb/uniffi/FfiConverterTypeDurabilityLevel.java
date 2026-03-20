package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDurabilityLevel implements FfiConverterRustBuffer<DurabilityLevel> {
    INSTANCE;

    @Override
    public DurabilityLevel read(ByteBuffer buf) {
        try {
            return DurabilityLevel.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(DurabilityLevel value) {
        return 4L;
    }

    @Override
    public void write(DurabilityLevel value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




