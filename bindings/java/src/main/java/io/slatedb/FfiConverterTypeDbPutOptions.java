package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbPutOptions implements FfiConverterRustBuffer<DbPutOptions> {
  INSTANCE;

  @Override
  public DbPutOptions read(ByteBuffer buf) {
    return new DbPutOptions(
      FfiConverterTypeTtl.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbPutOptions value) {
      return (
            FfiConverterTypeTtl.INSTANCE.allocationSize(value.ttl())
      );
  }

  @Override
  public void write(DbPutOptions value, ByteBuffer buf) {
      FfiConverterTypeTtl.INSTANCE.write(value.ttl(), buf);
  }
}



