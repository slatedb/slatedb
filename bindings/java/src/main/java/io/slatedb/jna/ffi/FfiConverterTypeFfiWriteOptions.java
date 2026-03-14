package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiWriteOptions implements FfiConverterRustBuffer<FfiWriteOptions> {
  INSTANCE;

  @Override
  public FfiWriteOptions read(ByteBuffer buf) {
    return new FfiWriteOptions(FfiConverterBoolean.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiWriteOptions value) {
    return (FfiConverterBoolean.INSTANCE.allocationSize(value.awaitDurable()));
  }

  @Override
  public void write(FfiWriteOptions value, ByteBuffer buf) {
    FfiConverterBoolean.INSTANCE.write(value.awaitDurable(), buf);
  }
}
