package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeWalFileMetadata implements FfiConverterRustBuffer<WalFileMetadata> {
  INSTANCE;

  @Override
  public WalFileMetadata read(ByteBuffer buf) {
    return new WalFileMetadata(
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterInteger.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterString.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(WalFileMetadata value) {
      return (
            FfiConverterLong.INSTANCE.allocationSize(value.lastModifiedSeconds()) +
            FfiConverterInteger.INSTANCE.allocationSize(value.lastModifiedNanos()) +
            FfiConverterLong.INSTANCE.allocationSize(value.sizeBytes()) +
            FfiConverterString.INSTANCE.allocationSize(value.location())
      );
  }

  @Override
  public void write(WalFileMetadata value, ByteBuffer buf) {
      FfiConverterLong.INSTANCE.write(value.lastModifiedSeconds(), buf);
      FfiConverterInteger.INSTANCE.write(value.lastModifiedNanos(), buf);
      FfiConverterLong.INSTANCE.write(value.sizeBytes(), buf);
      FfiConverterString.INSTANCE.write(value.location(), buf);
  }
}



