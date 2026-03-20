package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterInteger implements FfiConverter<Integer, Integer>{
  INSTANCE;

    @Override
    public Integer lift(Integer value) {
        return value;
    }

    @Override
    public Integer read(ByteBuffer buf) {
        return buf.getInt();
    }

    @Override
    public Integer lower(Integer value) {
        return value;
    }

    @Override
    public long allocationSize(Integer value) {
        return 4L;
    }

    @Override
    public void write(Integer value, ByteBuffer buf) {
        buf.putInt(value);
    }
}

