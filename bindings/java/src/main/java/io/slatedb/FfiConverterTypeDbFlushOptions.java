package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbFlushOptions implements FfiConverterRustBuffer<DbFlushOptions> {
  INSTANCE;

  @Override
  public DbFlushOptions read(ByteBuffer buf) {
    return new DbFlushOptions(
      FfiConverterTypeFlushType.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbFlushOptions value) {
      return (
            FfiConverterTypeFlushType.INSTANCE.allocationSize(value.flushType())
      );
  }

  @Override
  public void write(DbFlushOptions value, ByteBuffer buf) {
      FfiConverterTypeFlushType.INSTANCE.write(value.flushType(), buf);
  }
}



