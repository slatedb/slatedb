package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeTtl implements FfiConverterRustBuffer<Ttl> {
    INSTANCE;

    @Override
    public Ttl read(ByteBuffer buf) {
      return switch (buf.getInt()) {
        case 1 -> new Ttl.Default();
        case 2 -> new Ttl.NoExpiry();
        case 3 -> new Ttl.ExpireAfterTicks(FfiConverterLong.INSTANCE.read(buf)
          );
        default ->
          throw new RuntimeException("invalid enum value, something is very wrong!");
      };
    }

    @Override
    public long allocationSize(Ttl value) {
        return switch (value) {
          case Ttl.Default() ->
            (4L);
          case Ttl.NoExpiry() ->
            (4L);
          case Ttl.ExpireAfterTicks(var v1) ->
            (4L
            + FfiConverterLong.INSTANCE.allocationSize(v1));
        };
    }

    @Override
    public void write(Ttl value, ByteBuffer buf) {
      switch (value) {
        case Ttl.Default() -> {
          buf.putInt(1);
        }
        case Ttl.NoExpiry() -> {
          buf.putInt(2);
        }
        case Ttl.ExpireAfterTicks(var v1) -> {
          buf.putInt(3);
          FfiConverterLong.INSTANCE.write(v1, buf);
        }
      };
    }
}





