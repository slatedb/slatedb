package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiSstBlockSize implements FfiConverterRustBuffer<FfiSstBlockSize> {
  INSTANCE;

  @Override
  public FfiSstBlockSize read(ByteBuffer buf) {
    try {
      return FfiSstBlockSize.values()[buf.getInt() - 1];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("invalid enum value, something is very wrong!!", e);
    }
  }

  @Override
  public long allocationSize(FfiSstBlockSize value) {
    return 4L;
  }

  @Override
  public void write(FfiSstBlockSize value, ByteBuffer buf) {
    buf.putInt(value.ordinal() + 1);
  }
}
