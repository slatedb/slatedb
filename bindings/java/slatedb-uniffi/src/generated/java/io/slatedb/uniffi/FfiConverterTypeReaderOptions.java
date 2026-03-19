package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeReaderOptions implements FfiConverterRustBuffer<ReaderOptions> {
  INSTANCE;

  @Override
  public ReaderOptions read(ByteBuffer buf) {
    return new ReaderOptions(
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(ReaderOptions value) {
      return (
            FfiConverterLong.INSTANCE.allocationSize(value.manifestPollIntervalMs()) +
            FfiConverterLong.INSTANCE.allocationSize(value.checkpointLifetimeMs()) +
            FfiConverterLong.INSTANCE.allocationSize(value.maxMemtableBytes()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.skipWalReplay())
      );
  }

  @Override
  public void write(ReaderOptions value, ByteBuffer buf) {
      FfiConverterLong.INSTANCE.write(value.manifestPollIntervalMs(), buf);
      FfiConverterLong.INSTANCE.write(value.checkpointLifetimeMs(), buf);
      FfiConverterLong.INSTANCE.write(value.maxMemtableBytes(), buf);
      FfiConverterBoolean.INSTANCE.write(value.skipWalReplay(), buf);
  }
}



