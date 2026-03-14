package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiWalFileMetadata
    implements FfiConverterRustBuffer<FfiWalFileMetadata> {
  INSTANCE;

  @Override
  public FfiWalFileMetadata read(ByteBuffer buf) {
    return new FfiWalFileMetadata(
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterInteger.INSTANCE.read(buf),
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterString.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiWalFileMetadata value) {
    return (FfiConverterLong.INSTANCE.allocationSize(value.lastModifiedSeconds())
        + FfiConverterInteger.INSTANCE.allocationSize(value.lastModifiedNanos())
        + FfiConverterLong.INSTANCE.allocationSize(value.sizeBytes())
        + FfiConverterString.INSTANCE.allocationSize(value.location()));
  }

  @Override
  public void write(FfiWalFileMetadata value, ByteBuffer buf) {
    FfiConverterLong.INSTANCE.write(value.lastModifiedSeconds(), buf);
    FfiConverterInteger.INSTANCE.write(value.lastModifiedNanos(), buf);
    FfiConverterLong.INSTANCE.write(value.sizeBytes(), buf);
    FfiConverterString.INSTANCE.write(value.location(), buf);
  }
}
