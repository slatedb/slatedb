package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiFlushType implements FfiConverterRustBuffer<FfiFlushType> {
  INSTANCE;

  @Override
  public FfiFlushType read(ByteBuffer buf) {
    try {
      return FfiFlushType.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiFlushType value) {
    return 4L;
  }

  @Override
  public void write(FfiFlushType value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
