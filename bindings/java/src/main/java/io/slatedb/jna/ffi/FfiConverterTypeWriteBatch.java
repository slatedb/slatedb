package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.nio.ByteBuffer;

public enum FfiConverterTypeWriteBatch implements FfiConverter<WriteBatch, Pointer> {
  INSTANCE;

  @Override
  public Pointer lower(WriteBatch value) {
    return value.uniffiClonePointer();
  }

  @Override
  public WriteBatch lift(Pointer value) {
    return new WriteBatch(value);
  }

  @Override
  public WriteBatch read(ByteBuffer buf) {
    // The Rust code always writes pointers as 8 bytes, and will
    // fail to compile if they don't fit.
    return lift(new Pointer(buf.getLong()));
  }

  @Override
  public long allocationSize(WriteBatch value) {
    return 8L;
  }

  @Override
  public void write(WriteBatch value, ByteBuffer buf) {
    // The Rust code always expects pointers written as 8 bytes,
    // and will fail to compile if they don't fit.
    buf.putLong(Pointer.nativeValue(lower(value)));
  }
}
