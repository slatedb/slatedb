package io.slatedb;


import java.nio.ByteBuffer;
import com.sun.jna.Pointer;

public enum FfiConverterTypeObjectStore implements FfiConverter<ObjectStore, Pointer> {
    INSTANCE;

    @Override
    public Pointer lower(ObjectStore value) {
        return value.uniffiClonePointer();
    }

    @Override
    public ObjectStore lift(Pointer value) {
        return new ObjectStore(value);
    }

    @Override
    public ObjectStore read(ByteBuffer buf) {
        // The Rust code always writes pointers as 8 bytes, and will
        // fail to compile if they don't fit.
        return lift(new Pointer(buf.getLong()));
    }

    @Override
    public long allocationSize(ObjectStore value) {
      return 8L;
    }

    @Override
    public void write(ObjectStore value, ByteBuffer buf) {
        // The Rust code always expects pointers written as 8 bytes,
        // and will fail to compile if they don't fit.
        buf.putLong(Pointer.nativeValue(lower(value)));
    }
}



