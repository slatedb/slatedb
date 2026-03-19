package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "returnValue", "callStatus" })
public class UniffiForeignFutureStructI16 extends Structure {
    public short returnValue = (short)0;
    public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiForeignFutureStructI16() {
        super();
    }
    
    public UniffiForeignFutureStructI16(
        short returnValue,
        UniffiRustCallStatus.ByValue callStatus
    ) {
        this.returnValue = returnValue;
        this.callStatus = callStatus;
    }

    public static class UniffiByValue extends UniffiForeignFutureStructI16 implements Structure.ByValue {
        public UniffiByValue(
            short returnValue,
            UniffiRustCallStatus.ByValue callStatus
        ) {
            super(returnValue,        
            callStatus        
            );
        }
    }

    void uniffiSetValue(UniffiForeignFutureStructI16 other) {
        returnValue = other.returnValue;
        callStatus = other.callStatus;
    }

}
