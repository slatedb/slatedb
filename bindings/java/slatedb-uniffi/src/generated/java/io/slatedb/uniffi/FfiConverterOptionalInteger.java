package io.slatedb.uniffi;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

// public class TestForOptionals {}
public enum FfiConverterOptionalInteger implements FfiConverterRustBuffer<Integer> {
  INSTANCE;

  @Override
  public Integer read(ByteBuffer buf) {
    if (buf.get() == (byte)0) {
      return null;
    }
    return FfiConverterInteger.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(Integer value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterInteger.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(Integer value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte)0);
    } else {
      buf.put((byte)1);
      FfiConverterInteger.INSTANCE.write(value, buf);
    }
  }
}



