package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeMergeOptions implements FfiConverterRustBuffer<MergeOptions> {
  INSTANCE;

  @Override
  public MergeOptions read(ByteBuffer buf) {
    return new MergeOptions(
      FfiConverterTypeTtl.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(MergeOptions value) {
      return (
            FfiConverterTypeTtl.INSTANCE.allocationSize(value.ttl())
      );
  }

  @Override
  public void write(MergeOptions value, ByteBuffer buf) {
      FfiConverterTypeTtl.INSTANCE.write(value.ttl(), buf);
  }
}



