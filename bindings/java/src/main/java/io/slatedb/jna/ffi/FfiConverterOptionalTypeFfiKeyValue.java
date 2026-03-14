package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeFfiKeyValue implements FfiConverterRustBuffer<FfiKeyValue> {
  INSTANCE;

  @Override
  public FfiKeyValue read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeFfiKeyValue.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(FfiKeyValue value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeFfiKeyValue.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(FfiKeyValue value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeFfiKeyValue.INSTANCE.write(value, buf);
    }
  }
}
