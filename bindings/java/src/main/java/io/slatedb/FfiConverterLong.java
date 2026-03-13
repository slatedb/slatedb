package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterLong implements FfiConverter<Long, Long> {
    INSTANCE;

    @Override
    public Long lift(Long value) {
        return value;
    }

    @Override
    public Long read(ByteBuffer buf) {
        return buf.getLong();
    }

    @Override
    public Long lower(Long value) {
        return value;
    }

    @Override
    public long allocationSize(Long value) {
        return 8L;
    }

    @Override
    public void write(Long value, ByteBuffer buf) {
        buf.putLong(value);
    }
}


