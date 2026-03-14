package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiWriteBatch implements FfiConverter<FfiWriteBatch, Pointer> {
  INSTANCE;

  @Override
  public Pointer lower(FfiWriteBatch value) {
    return value.uniffiClonePointer();
  }

  @Override
  public FfiWriteBatch lift(Pointer value) {
    return new FfiWriteBatch(value);
  }

  @Override
  public FfiWriteBatch read(ByteBuffer buf) {
    // The Rust code always writes pointers as 8 bytes, and will
    // fail to compile if they don't fit.
    return lift(new Pointer(buf.getLong()));
  }

  @Override
  public long allocationSize(FfiWriteBatch value) {
    return 8L;
  }

  @Override
  public void write(FfiWriteBatch value, ByteBuffer buf) {
    // The Rust code always expects pointers written as 8 bytes,
    // and will fail to compile if they don't fit.
    buf.putLong(Pointer.nativeValue(lower(value)));
  }
}
