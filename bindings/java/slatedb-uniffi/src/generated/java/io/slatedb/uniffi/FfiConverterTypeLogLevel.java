package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeLogLevel implements FfiConverterRustBuffer<LogLevel> {
    INSTANCE;

    @Override
    public LogLevel read(ByteBuffer buf) {
        try {
            return LogLevel.values()[buf.getInt() - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("invalid enum value, something is very wrong!!", e);
        }
    }

    @Override
    public long allocationSize(LogLevel value) {
        return 4L;
    }

    @Override
    public void write(LogLevel value, ByteBuffer buf) {
        buf.putInt(value.ordinal() + 1);
    }
}




