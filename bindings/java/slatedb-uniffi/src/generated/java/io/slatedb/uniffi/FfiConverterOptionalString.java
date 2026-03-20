package io.slatedb.uniffi;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

// public class TestForOptionals {}
public enum FfiConverterOptionalString implements FfiConverterRustBuffer<String> {
  INSTANCE;

  @Override
  public String read(ByteBuffer buf) {
    if (buf.get() == (byte)0) {
      return null;
    }
    return FfiConverterString.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(String value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterString.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(String value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte)0);
    } else {
      buf.put((byte)1);
      FfiConverterString.INSTANCE.write(value, buf);
    }
  }
}



