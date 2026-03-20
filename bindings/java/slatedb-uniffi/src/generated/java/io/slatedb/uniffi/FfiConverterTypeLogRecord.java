package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeLogRecord implements FfiConverterRustBuffer<LogRecord> {
  INSTANCE;

  @Override
  public LogRecord read(ByteBuffer buf) {
    return new LogRecord(
      FfiConverterTypeLogLevel.INSTANCE.read(buf),
      FfiConverterString.INSTANCE.read(buf),
      FfiConverterString.INSTANCE.read(buf),
      FfiConverterOptionalString.INSTANCE.read(buf),
      FfiConverterOptionalString.INSTANCE.read(buf),
      FfiConverterOptionalInteger.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(LogRecord value) {
      return (
            FfiConverterTypeLogLevel.INSTANCE.allocationSize(value.level()) +
            FfiConverterString.INSTANCE.allocationSize(value.target()) +
            FfiConverterString.INSTANCE.allocationSize(value.message()) +
            FfiConverterOptionalString.INSTANCE.allocationSize(value.modulePath()) +
            FfiConverterOptionalString.INSTANCE.allocationSize(value.file()) +
            FfiConverterOptionalInteger.INSTANCE.allocationSize(value.line())
      );
  }

  @Override
  public void write(LogRecord value, ByteBuffer buf) {
      FfiConverterTypeLogLevel.INSTANCE.write(value.level(), buf);
      FfiConverterString.INSTANCE.write(value.target(), buf);
      FfiConverterString.INSTANCE.write(value.message(), buf);
      FfiConverterOptionalString.INSTANCE.write(value.modulePath(), buf);
      FfiConverterOptionalString.INSTANCE.write(value.file(), buf);
      FfiConverterOptionalInteger.INSTANCE.write(value.line(), buf);
  }
}



