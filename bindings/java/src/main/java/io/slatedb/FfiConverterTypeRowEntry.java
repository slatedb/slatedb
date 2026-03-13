package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeRowEntry implements FfiConverterRustBuffer<RowEntry> {
  INSTANCE;

  @Override
  public RowEntry read(ByteBuffer buf) {
    return new RowEntry(
      FfiConverterTypeRowEntryKind.INSTANCE.read(buf),
      FfiConverterByteArray.INSTANCE.read(buf),
      FfiConverterOptionalByteArray.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterOptionalLong.INSTANCE.read(buf),
      FfiConverterOptionalLong.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(RowEntry value) {
      return (
            FfiConverterTypeRowEntryKind.INSTANCE.allocationSize(value.kind()) +
            FfiConverterByteArray.INSTANCE.allocationSize(value.key()) +
            FfiConverterOptionalByteArray.INSTANCE.allocationSize(value.value()) +
            FfiConverterLong.INSTANCE.allocationSize(value.seq()) +
            FfiConverterOptionalLong.INSTANCE.allocationSize(value.createTs()) +
            FfiConverterOptionalLong.INSTANCE.allocationSize(value.expireTs())
      );
  }

  @Override
  public void write(RowEntry value, ByteBuffer buf) {
      FfiConverterTypeRowEntryKind.INSTANCE.write(value.kind(), buf);
      FfiConverterByteArray.INSTANCE.write(value.key(), buf);
      FfiConverterOptionalByteArray.INSTANCE.write(value.value(), buf);
      FfiConverterLong.INSTANCE.write(value.seq(), buf);
      FfiConverterOptionalLong.INSTANCE.write(value.createTs(), buf);
      FfiConverterOptionalLong.INSTANCE.write(value.expireTs(), buf);
  }
}



