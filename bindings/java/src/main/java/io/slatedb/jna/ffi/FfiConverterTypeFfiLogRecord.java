package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiLogRecord implements FfiConverterRustBuffer<FfiLogRecord> {
  INSTANCE;

  @Override
  public FfiLogRecord read(ByteBuffer buf) {
    return new FfiLogRecord(
        FfiConverterTypeFfiLogLevel.INSTANCE.read(buf),
        FfiConverterString.INSTANCE.read(buf),
        FfiConverterString.INSTANCE.read(buf),
        FfiConverterOptionalString.INSTANCE.read(buf),
        FfiConverterOptionalString.INSTANCE.read(buf),
        FfiConverterOptionalInteger.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiLogRecord value) {
    return (FfiConverterTypeFfiLogLevel.INSTANCE.allocationSize(value.level())
        + FfiConverterString.INSTANCE.allocationSize(value.target())
        + FfiConverterString.INSTANCE.allocationSize(value.message())
        + FfiConverterOptionalString.INSTANCE.allocationSize(value.modulePath())
        + FfiConverterOptionalString.INSTANCE.allocationSize(value.file())
        + FfiConverterOptionalInteger.INSTANCE.allocationSize(value.line()));
  }

  @Override
  public void write(FfiLogRecord value, ByteBuffer buf) {
    FfiConverterTypeFfiLogLevel.INSTANCE.write(value.level(), buf);
    FfiConverterString.INSTANCE.write(value.target(), buf);
    FfiConverterString.INSTANCE.write(value.message(), buf);
    FfiConverterOptionalString.INSTANCE.write(value.modulePath(), buf);
    FfiConverterOptionalString.INSTANCE.write(value.file(), buf);
    FfiConverterOptionalInteger.INSTANCE.write(value.line(), buf);
  }
}
