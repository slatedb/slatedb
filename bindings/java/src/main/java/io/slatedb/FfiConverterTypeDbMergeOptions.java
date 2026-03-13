package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbMergeOptions implements FfiConverterRustBuffer<DbMergeOptions> {
  INSTANCE;

  @Override
  public DbMergeOptions read(ByteBuffer buf) {
    return new DbMergeOptions(
      FfiConverterTypeTtl.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbMergeOptions value) {
      return (
            FfiConverterTypeTtl.INSTANCE.allocationSize(value.ttl())
      );
  }

  @Override
  public void write(DbMergeOptions value, ByteBuffer buf) {
      FfiConverterTypeTtl.INSTANCE.write(value.ttl(), buf);
  }
}



