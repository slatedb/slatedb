package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeReadOptions implements FfiConverterRustBuffer<ReadOptions> {
  INSTANCE;

  @Override
  public ReadOptions read(ByteBuffer buf) {
    return new ReadOptions(
        FfiConverterTypeDurabilityLevel.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(ReadOptions value) {
    return (FfiConverterTypeDurabilityLevel.INSTANCE.allocationSize(value.durabilityFilter())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.dirty())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.cacheBlocks()));
  }

  @Override
  public void write(ReadOptions value, ByteBuffer buf) {
    FfiConverterTypeDurabilityLevel.INSTANCE.write(value.durabilityFilter(), buf);
    FfiConverterBoolean.INSTANCE.write(value.dirty(), buf);
    FfiConverterBoolean.INSTANCE.write(value.cacheBlocks(), buf);
  }
}
