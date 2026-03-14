package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiIsolationLevel implements FfiConverterRustBuffer<FfiIsolationLevel> {
  INSTANCE;

  @Override
  public FfiIsolationLevel read(ByteBuffer buf) {
    try {
      return FfiIsolationLevel.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiIsolationLevel value) {
    return 4L;
  }

  @Override
  public void write(FfiIsolationLevel value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
