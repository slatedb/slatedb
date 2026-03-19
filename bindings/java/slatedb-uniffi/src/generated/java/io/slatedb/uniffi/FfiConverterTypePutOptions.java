package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypePutOptions implements FfiConverterRustBuffer<PutOptions> {
  INSTANCE;

  @Override
  public PutOptions read(ByteBuffer buf) {
    return new PutOptions(
      FfiConverterTypeTtl.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(PutOptions value) {
      return (
            FfiConverterTypeTtl.INSTANCE.allocationSize(value.ttl())
      );
  }

  @Override
  public void write(PutOptions value, ByteBuffer buf) {
      FfiConverterTypeTtl.INSTANCE.write(value.ttl(), buf);
  }
}



