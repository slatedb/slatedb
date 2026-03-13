package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeIsolationLevel implements FfiConverterRustBuffer<IsolationLevel> {
    INSTANCE;

    @Override
    public IsolationLevel read(ByteBuffer buf) {
        try {
            return IsolationLevel.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(IsolationLevel value) {
        return 4L;
    }

    @Override
    public void write(IsolationLevel value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




