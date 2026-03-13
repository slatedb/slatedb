package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeSstBlockSize implements FfiConverterRustBuffer<SstBlockSize> {
    INSTANCE;

    @Override
    public SstBlockSize read(ByteBuffer buf) {
        try {
            return SstBlockSize.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(SstBlockSize value) {
        return 4L;
    }

    @Override
    public void write(SstBlockSize value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




