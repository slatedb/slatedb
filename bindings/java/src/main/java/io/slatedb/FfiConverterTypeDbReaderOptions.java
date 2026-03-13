package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbReaderOptions implements FfiConverterRustBuffer<DbReaderOptions> {
  INSTANCE;

  @Override
  public DbReaderOptions read(ByteBuffer buf) {
    return new DbReaderOptions(
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterBoolean.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(DbReaderOptions value) {
      return (
            FfiConverterLong.INSTANCE.allocationSize(value.manifestPollIntervalMs()) +
            FfiConverterLong.INSTANCE.allocationSize(value.checkpointLifetimeMs()) +
            FfiConverterLong.INSTANCE.allocationSize(value.maxMemtableBytes()) +
            FfiConverterBoolean.INSTANCE.allocationSize(value.skipWalReplay())
      );
  }

  @Override
  public void write(DbReaderOptions value, ByteBuffer buf) {
      FfiConverterLong.INSTANCE.write(value.manifestPollIntervalMs(), buf);
      FfiConverterLong.INSTANCE.write(value.checkpointLifetimeMs(), buf);
      FfiConverterLong.INSTANCE.write(value.maxMemtableBytes(), buf);
      FfiConverterBoolean.INSTANCE.write(value.skipWalReplay(), buf);
  }
}



