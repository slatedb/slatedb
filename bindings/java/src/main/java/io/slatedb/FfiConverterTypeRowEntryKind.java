package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeRowEntryKind implements FfiConverterRustBuffer<RowEntryKind> {
    INSTANCE;

    @Override
    public RowEntryKind read(ByteBuffer buf) {
        try {
            return RowEntryKind.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(RowEntryKind value) {
        return 4L;
    }

    @Override
    public void write(RowEntryKind value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




