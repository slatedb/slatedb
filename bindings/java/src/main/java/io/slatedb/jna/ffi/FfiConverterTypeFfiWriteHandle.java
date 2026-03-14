package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiWriteHandle implements FfiConverterRustBuffer<FfiWriteHandle> {
  INSTANCE;

  @Override
  public FfiWriteHandle read(ByteBuffer buf) {
    return new FfiWriteHandle(
        FfiConverterLong.INSTANCE.read(buf), FfiConverterLong.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiWriteHandle value) {
    return (FfiConverterLong.INSTANCE.allocationSize(value.seqnum())
        + FfiConverterLong.INSTANCE.allocationSize(value.createTs()));
  }

  @Override
  public void write(FfiWriteHandle value, ByteBuffer buf) {
    FfiConverterLong.INSTANCE.write(value.seqnum(), buf);
    FfiConverterLong.INSTANCE.write(value.createTs(), buf);
  }
}
