package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeKeyRange implements FfiConverterRustBuffer<KeyRange> {
  INSTANCE;

  @Override
  public KeyRange read(ByteBuffer buf) {
    return new KeyRange(
        FfiConverterOptionalByteArray.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf),
        FfiConverterOptionalByteArray.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(KeyRange value) {
    return (FfiConverterOptionalByteArray.INSTANCE.allocationSize(value.start())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.startInclusive())
        + FfiConverterOptionalByteArray.INSTANCE.allocationSize(value.end())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.endInclusive()));
  }

  @Override
  public void write(KeyRange value, ByteBuffer buf) {
    FfiConverterOptionalByteArray.INSTANCE.write(value.start(), buf);
    FfiConverterBoolean.INSTANCE.write(value.startInclusive(), buf);
    FfiConverterOptionalByteArray.INSTANCE.write(value.end(), buf);
    FfiConverterBoolean.INSTANCE.write(value.endInclusive(), buf);
  }
}
