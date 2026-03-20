package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "handle", "free" })
public class UniffiForeignFuture extends Structure {
    public long handle = 0L;
    public UniffiForeignFutureFree free = null;

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiForeignFuture() {
        super();
    }
    
    public UniffiForeignFuture(
        long handle,
        UniffiForeignFutureFree free
    ) {
        this.handle = handle;
        this.free = free;
    }

    public static class UniffiByValue extends UniffiForeignFuture implements Structure.ByValue {
        public UniffiByValue(
            long handle,
            UniffiForeignFutureFree free
        ) {
            super(handle,        
            free        
            );
        }
    }

    void uniffiSetValue(UniffiForeignFuture other) {
        handle = other.handle;
        free = other.free;
    }

}
