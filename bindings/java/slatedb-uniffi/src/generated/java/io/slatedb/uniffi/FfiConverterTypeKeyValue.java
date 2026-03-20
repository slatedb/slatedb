package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeKeyValue implements FfiConverterRustBuffer<KeyValue> {
  INSTANCE;

  @Override
  public KeyValue read(ByteBuffer buf) {
    return new KeyValue(
      FfiConverterByteArray.INSTANCE.read(buf),
      FfiConverterByteArray.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterOptionalLong.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(KeyValue value) {
      return (
            FfiConverterByteArray.INSTANCE.allocationSize(value.key()) +
            FfiConverterByteArray.INSTANCE.allocationSize(value.value()) +
            FfiConverterLong.INSTANCE.allocationSize(value.seq()) +
            FfiConverterLong.INSTANCE.allocationSize(value.createTs()) +
            FfiConverterOptionalLong.INSTANCE.allocationSize(value.expireTs())
      );
  }

  @Override
  public void write(KeyValue value, ByteBuffer buf) {
      FfiConverterByteArray.INSTANCE.write(value.key(), buf);
      FfiConverterByteArray.INSTANCE.write(value.value(), buf);
      FfiConverterLong.INSTANCE.write(value.seq(), buf);
      FfiConverterLong.INSTANCE.write(value.createTs(), buf);
      FfiConverterOptionalLong.INSTANCE.write(value.expireTs(), buf);
  }
}



