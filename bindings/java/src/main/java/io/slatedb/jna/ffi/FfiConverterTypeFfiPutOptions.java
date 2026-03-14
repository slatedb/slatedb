package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiPutOptions implements FfiConverterRustBuffer<FfiPutOptions> {
  INSTANCE;

  @Override
  public FfiPutOptions read(ByteBuffer buf) {
    return new FfiPutOptions(FfiConverterTypeFfiTtl.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiPutOptions value) {
    return (FfiConverterTypeFfiTtl.INSTANCE.allocationSize(value.ttl()));
  }

  @Override
  public void write(FfiPutOptions value, ByteBuffer buf) {
    FfiConverterTypeFfiTtl.INSTANCE.write(value.ttl(), buf);
  }
}
