package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiCloseReason implements FfiConverterRustBuffer<FfiCloseReason> {
  INSTANCE;

  @Override
  public FfiCloseReason read(ByteBuffer buf) {
    try {
      return FfiCloseReason.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiCloseReason value) {
    return 4L;
  }

  @Override
  public void write(FfiCloseReason value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
