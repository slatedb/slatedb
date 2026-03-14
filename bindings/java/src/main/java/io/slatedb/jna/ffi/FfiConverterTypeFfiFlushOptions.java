package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiFlushOptions implements FfiConverterRustBuffer<FfiFlushOptions> {
  INSTANCE;

  @Override
  public FfiFlushOptions read(ByteBuffer buf) {
    return new FfiFlushOptions(FfiConverterTypeFfiFlushType.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiFlushOptions value) {
    return (FfiConverterTypeFfiFlushType.INSTANCE.allocationSize(value.flushType()));
  }

  @Override
  public void write(FfiFlushOptions value, ByteBuffer buf) {
    FfiConverterTypeFfiFlushType.INSTANCE.write(value.flushType(), buf);
  }
}
