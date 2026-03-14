package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiDurabilityLevel
    implements FfiConverterRustBuffer<FfiDurabilityLevel> {
  INSTANCE;

  @Override
  public FfiDurabilityLevel read(ByteBuffer buf) {
    try {
      return FfiDurabilityLevel.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiDurabilityLevel value) {
    return 4L;
  }

  @Override
  public void write(FfiDurabilityLevel value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
