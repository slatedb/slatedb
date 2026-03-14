package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeFfiLogCallback
    implements FfiConverterRustBuffer<FfiLogCallback> {
  INSTANCE;

  @Override
  public FfiLogCallback read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeFfiLogCallback.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(FfiLogCallback value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeFfiLogCallback.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(FfiLogCallback value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeFfiLogCallback.INSTANCE.write(value, buf);
    }
  }
}
