package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeLogCallback implements FfiConverterRustBuffer<LogCallback> {
  INSTANCE;

  @Override
  public LogCallback read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeLogCallback.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(LogCallback value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeLogCallback.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(LogCallback value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeLogCallback.INSTANCE.write(value, buf);
    }
  }
}
