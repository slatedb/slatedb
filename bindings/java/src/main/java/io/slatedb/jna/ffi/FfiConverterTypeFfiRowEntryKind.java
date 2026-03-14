package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiRowEntryKind implements FfiConverterRustBuffer<FfiRowEntryKind> {
  INSTANCE;

  @Override
  public FfiRowEntryKind read(ByteBuffer buf) {
    try {
      return FfiRowEntryKind.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiRowEntryKind value) {
    return 4L;
  }

  @Override
  public void write(FfiRowEntryKind value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
