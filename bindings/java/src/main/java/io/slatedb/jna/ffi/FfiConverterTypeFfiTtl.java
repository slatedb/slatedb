package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiTtl implements FfiConverterRustBuffer<FfiTtl> {
  INSTANCE;

  @Override
  public FfiTtl read(ByteBuffer buf) {
    return switch (buf.getInt()) {
      case 1 -> new FfiTtl.Default();
      case 2 -> new FfiTtl.NoExpiry();
      case 3 -> new FfiTtl.ExpireAfterTicks(FfiConverterLong.INSTANCE.read(buf));
      default -> throw new RuntimeException("invalid enum value, something is very wrong!");
    };
  }

  @Override
  public long allocationSize(FfiTtl value) {
    return switch (value) {
      case FfiTtl.Default() -> (4L);
      case FfiTtl.NoExpiry() -> (4L);
      case FfiTtl.ExpireAfterTicks(var v1) -> (4L + FfiConverterLong.INSTANCE.allocationSize(v1));
    };
  }

  @Override
  public void write(FfiTtl value, ByteBuffer buf) {
    switch (value) {
      case FfiTtl.Default() -> {
        buf.putInt(1);
      }
      case FfiTtl.NoExpiry() -> {
        buf.putInt(2);
      }
      case FfiTtl.ExpireAfterTicks(var v1) -> {
        buf.putInt(3);
        FfiConverterLong.INSTANCE.write(v1, buf);
      }
    }
    ;
  }
}
