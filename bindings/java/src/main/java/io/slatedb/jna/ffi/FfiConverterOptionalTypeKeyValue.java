package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeKeyValue implements FfiConverterRustBuffer<KeyValue> {
  INSTANCE;

  @Override
  public KeyValue read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeKeyValue.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(KeyValue value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeKeyValue.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(KeyValue value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeKeyValue.INSTANCE.write(value, buf);
    }
  }
}
