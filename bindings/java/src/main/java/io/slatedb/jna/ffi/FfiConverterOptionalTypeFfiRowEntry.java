package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeFfiRowEntry implements FfiConverterRustBuffer<FfiRowEntry> {
  INSTANCE;

  @Override
  public FfiRowEntry read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeFfiRowEntry.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(FfiRowEntry value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeFfiRowEntry.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(FfiRowEntry value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeFfiRowEntry.INSTANCE.write(value, buf);
    }
  }
}
