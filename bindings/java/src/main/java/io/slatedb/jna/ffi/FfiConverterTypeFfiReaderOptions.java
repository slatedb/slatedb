package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiReaderOptions implements FfiConverterRustBuffer<FfiReaderOptions> {
  INSTANCE;

  @Override
  public FfiReaderOptions read(ByteBuffer buf) {
    return new FfiReaderOptions(
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterLong.INSTANCE.read(buf),
        FfiConverterBoolean.INSTANCE.read(buf));
  }

  @Override
  public long allocationSize(FfiReaderOptions value) {
    return (FfiConverterLong.INSTANCE.allocationSize(value.manifestPollIntervalMs())
        + FfiConverterLong.INSTANCE.allocationSize(value.checkpointLifetimeMs())
        + FfiConverterLong.INSTANCE.allocationSize(value.maxMemtableBytes())
        + FfiConverterBoolean.INSTANCE.allocationSize(value.skipWalReplay()));
  }

  @Override
  public void write(FfiReaderOptions value, ByteBuffer buf) {
    FfiConverterLong.INSTANCE.write(value.manifestPollIntervalMs(), buf);
    FfiConverterLong.INSTANCE.write(value.checkpointLifetimeMs(), buf);
    FfiConverterLong.INSTANCE.write(value.maxMemtableBytes(), buf);
    FfiConverterBoolean.INSTANCE.write(value.skipWalReplay(), buf);
  }
}
