package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiMergeOptions implements FfiConverterRustBuffer<FfiMergeOptions> {
  INSTANCE;

  @Override
  public FfiMergeOptions read(ByteBuffer buf) {
    return new FfiMergeOptions(FfiConverterTypeFfiTtl.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiMergeOptions value) {
    return (FfiConverterTypeFfiTtl.INSTANCE.allocationSize(value.ttl()));
  }

  @Override
  public void write(FfiMergeOptions value, ByteBuffer buf) {
    FfiConverterTypeFfiTtl.INSTANCE.write(value.ttl(), buf);
  }
}
