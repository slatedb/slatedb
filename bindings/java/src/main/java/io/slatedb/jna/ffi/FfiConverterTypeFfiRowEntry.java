package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiRowEntry implements FfiConverterRustBuffer<FfiRowEntry> {
  INSTANCE;

  @Override
  public FfiRowEntry read(ByteBuffer buf) {
    return new FfiRowEntry(
        FfiConverterTypeFfiRowEntryKind.INSTANCE.read(buf),
        FfiConverterByteArray.INSTANCE.read(buf),
        FfiConverterOptionalByteArray.INSTANCE.read(buf),
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterOptionalLong.INSTANCE.read(buf),
        FfiConverterOptionalLong.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiRowEntry value) {
    return (FfiConverterTypeFfiRowEntryKind.INSTANCE.allocationSize(value.kind())
        + FfiConverterByteArray.INSTANCE.allocationSize(value.key())
        + FfiConverterOptionalByteArray.INSTANCE.allocationSize(value.value())
        + FfiConverterLong.INSTANCE.allocationSize(value.seq())
        + FfiConverterOptionalLong.INSTANCE.allocationSize(value.createTs())
        + FfiConverterOptionalLong.INSTANCE.allocationSize(value.expireTs()));
  }

  @Override
  public void write(FfiRowEntry value, ByteBuffer buf) {
    FfiConverterTypeFfiRowEntryKind.INSTANCE.write(value.kind(), buf);
    FfiConverterByteArray.INSTANCE.write(value.key(), buf);
    FfiConverterOptionalByteArray.INSTANCE.write(value.value(), buf);
    FfiConverterLong.INSTANCE.write(value.seq(), buf);
    FfiConverterOptionalLong.INSTANCE.write(value.createTs(), buf);
    FfiConverterOptionalLong.INSTANCE.write(value.expireTs(), buf);
  }
}
