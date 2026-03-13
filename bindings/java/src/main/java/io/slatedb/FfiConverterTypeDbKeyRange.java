package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbKeyRange implements FfiConverterRustBuffer<DbKeyRange> {
  INSTANCE;

  @Override
  public DbKeyRange read(ByteBuffer buf) {
    return new DbKeyRange(
      FfiConverterOptionalByteArray.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf),
      FfiConverterOptionalByteArray.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbKeyRange value) {
      return (
            FfiConverterOptionalByteArray.INSTANCE.allocationSize(value.start()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.startInclusive()) +
            FfiConverterOptionalByteArray.INSTANCE.allocationSize(value.end()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.endInclusive())
      );
  }

  @Override
  public void write(DbKeyRange value, ByteBuffer buf) {
      FfiConverterOptionalByteArray.INSTANCE.write(value.start(), buf);
      FfiConverterBoolean.INSTANCE.write(value.startInclusive(), buf);
      FfiConverterOptionalByteArray.INSTANCE.write(value.end(), buf);
      FfiConverterBoolean.INSTANCE.write(value.endInclusive(), buf);
  }
}



