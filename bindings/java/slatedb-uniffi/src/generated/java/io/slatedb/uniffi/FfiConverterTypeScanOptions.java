package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeScanOptions implements FfiConverterRustBuffer<ScanOptions> {
  INSTANCE;

  @Override
  public ScanOptions read(ByteBuffer buf) {
    return new ScanOptions(
      FfiConverterTypeDurabilityLevel.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(ScanOptions value) {
      return (
            FfiConverterTypeDurabilityLevel.INSTANCE.allocationSize(value.durabilityFilter()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.dirty()) +
            FfiConverterLong.INSTANCE.allocationSize(value.readAheadBytes()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.cacheBlocks()) +
            FfiConverterLong.INSTANCE.allocationSize(value.maxFetchTasks())
      );
  }

  @Override
  public void write(ScanOptions value, ByteBuffer buf) {
      FfiConverterTypeDurabilityLevel.INSTANCE.write(value.durabilityFilter(), buf);
      FfiConverterBoolean.INSTANCE.write(value.dirty(), buf);
      FfiConverterLong.INSTANCE.write(value.readAheadBytes(), buf);
      FfiConverterBoolean.INSTANCE.write(value.cacheBlocks(), buf);
      FfiConverterLong.INSTANCE.write(value.maxFetchTasks(), buf);
  }
}



