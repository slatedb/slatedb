package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeRowEntry implements FfiConverterRustBuffer<RowEntry> {
  INSTANCE;

  @Override
  public RowEntry read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeRowEntry.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(RowEntry value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeRowEntry.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(RowEntry value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeRowEntry.INSTANCE.write(value, buf);
    }
  }
}
