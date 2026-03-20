package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeFlushOptions implements FfiConverterRustBuffer<FlushOptions> {
  INSTANCE;

  @Override
  public FlushOptions read(ByteBuffer buf) {
    return new FlushOptions(
      FfiConverterTypeFlushType.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(FlushOptions value) {
      return (
            FfiConverterTypeFlushType.INSTANCE.allocationSize(value.flushType())
      );
  }

  @Override
  public void write(FlushOptions value, ByteBuffer buf) {
      FfiConverterTypeFlushType.INSTANCE.write(value.flushType(), buf);
  }
}



