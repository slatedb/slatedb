package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbReadOptions implements FfiConverterRustBuffer<DbReadOptions> {
  INSTANCE;

  @Override
  public DbReadOptions read(ByteBuffer buf) {
    return new DbReadOptions(
      FfiConverterTypeDurabilityLevel.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbReadOptions value) {
      return (
            FfiConverterTypeDurabilityLevel.INSTANCE.allocationSize(value.durabilityFilter()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.dirty()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.cacheBlocks())
      );
  }

  @Override
  public void write(DbReadOptions value, ByteBuffer buf) {
      FfiConverterTypeDurabilityLevel.INSTANCE.write(value.durabilityFilter(), buf);
      FfiConverterBoolean.INSTANCE.write(value.dirty(), buf);
      FfiConverterBoolean.INSTANCE.write(value.cacheBlocks(), buf);
  }
}



