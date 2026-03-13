package io.slatedb;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

// public class TestForOptionals {}
public enum FfiConverterOptionalLong implements FfiConverterRustBuffer<Long> {
  INSTANCE;

  @Override
  public Long read(ByteBuffer buf) {
    if (buf.get() == (byte)0) {
      return null;
    }
    return FfiConverterLong.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(Long value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterLong.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(Long value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte)0);
    } else {
      buf.put((byte)1);
      FfiConverterLong.INSTANCE.write(value, buf);
    }
  }
}



