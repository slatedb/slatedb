package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbWriteOptions implements FfiConverterRustBuffer<DbWriteOptions> {
  INSTANCE;

  @Override
  public DbWriteOptions read(ByteBuffer buf) {
    return new DbWriteOptions(
      FfiConverterBoolean.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbWriteOptions value) {
      return (
            FfiConverterBoolean.INSTANCE.allocationSize(value.awaitDurable())
      );
  }

  @Override
  public void write(DbWriteOptions value, ByteBuffer buf) {
      FfiConverterBoolean.INSTANCE.write(value.awaitDurable(), buf);
  }
}



