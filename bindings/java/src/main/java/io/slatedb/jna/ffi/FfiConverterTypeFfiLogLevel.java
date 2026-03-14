package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiLogLevel implements FfiConverterRustBuffer<FfiLogLevel> {
  INSTANCE;

  @Override
  public FfiLogLevel read(ByteBuffer buf) {
    try {
      return FfiLogLevel.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiLogLevel value) {
    return 4L;
  }

  @Override
  public void write(FfiLogLevel value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
