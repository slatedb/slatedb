package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeWriteOptions implements FfiConverterRustBuffer<WriteOptions> {
  INSTANCE;

  @Override
  public WriteOptions read(ByteBuffer buf) {
    return new WriteOptions(FfiConverterBoolean.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(WriteOptions value) {
    return (FfiConverterBoolean.INSTANCE.allocationSize(value.awaitDurable()));
  }

  @Override
  public void write(WriteOptions value, ByteBuffer buf) {
    FfiConverterBoolean.INSTANCE.write(value.awaitDurable(), buf);
  }
}
