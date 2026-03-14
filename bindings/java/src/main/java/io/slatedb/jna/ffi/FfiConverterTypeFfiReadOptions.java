package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiReadOptions implements FfiConverterRustBuffer<FfiReadOptions> {
  INSTANCE;

  @Override
  public FfiReadOptions read(ByteBuffer buf) {
    return new FfiReadOptions(
        FfiConverterTypeFfiDurabilityLevel.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiReadOptions value) {
    return (FfiConverterTypeFfiDurabilityLevel.INSTANCE.allocationSize(value.durabilityFilter())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.dirty())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.cacheBlocks()));
  }

  @Override
  public void write(FfiReadOptions value, ByteBuffer buf) {
    FfiConverterTypeFfiDurabilityLevel.INSTANCE.write(value.durabilityFilter(), buf);
    FfiConverterBoolean.INSTANCE.write(value.dirty(), buf);
    FfiConverterBoolean.INSTANCE.write(value.cacheBlocks(), buf);
  }
}
