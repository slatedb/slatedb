package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbScanOptions implements FfiConverterRustBuffer<DbScanOptions> {
  INSTANCE;

  @Override
  public DbScanOptions read(ByteBuffer buf) {
    return new DbScanOptions(
      FfiConverterTypeDurabilityLevel.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbScanOptions value) {
      return (
            FfiConverterTypeDurabilityLevel.INSTANCE.allocationSize(value.durabilityFilter()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.dirty()) +
            FfiConverterLong.INSTANCE.allocationSize(value.readAheadBytes()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.cacheBlocks()) +
            FfiConverterLong.INSTANCE.allocationSize(value.maxFetchTasks())
      );
  }

  @Override
  public void write(DbScanOptions value, ByteBuffer buf) {
      FfiConverterTypeDurabilityLevel.INSTANCE.write(value.durabilityFilter(), buf);
      FfiConverterBoolean.INSTANCE.write(value.dirty(), buf);
      FfiConverterLong.INSTANCE.write(value.readAheadBytes(), buf);
      FfiConverterBoolean.INSTANCE.write(value.cacheBlocks(), buf);
      FfiConverterLong.INSTANCE.write(value.maxFetchTasks(), buf);
  }
}



