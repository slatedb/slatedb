package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiScanOptions implements FfiConverterRustBuffer<FfiScanOptions> {
  INSTANCE;

  @Override
  public FfiScanOptions read(ByteBuffer buf) {
    return new FfiScanOptions(
        FfiConverterTypeFfiDurabilityLevel.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf),
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf),
        FfiConverterLong.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiScanOptions value) {
    return (FfiConverterTypeFfiDurabilityLevel.INSTANCE.allocationSize(value.durabilityFilter())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.dirty())
        + FfiConverterLong.INSTANCE.allocationSize(value.readAheadBytes())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.cacheBlocks())
        + FfiConverterLong.INSTANCE.allocationSize(value.maxFetchTasks()));
  }

  @Override
  public void write(FfiScanOptions value, ByteBuffer buf) {
    FfiConverterTypeFfiDurabilityLevel.INSTANCE.write(value.durabilityFilter(), buf);
    FfiConverterBoolean.INSTANCE.write(value.dirty(), buf);
    FfiConverterLong.INSTANCE.write(value.readAheadBytes(), buf);
    FfiConverterBoolean.INSTANCE.write(value.cacheBlocks(), buf);
    FfiConverterLong.INSTANCE.write(value.maxFetchTasks(), buf);
  }
}
