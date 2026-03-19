package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "returnValue", "callStatus" })
public class UniffiForeignFutureStructU16 extends Structure {
    public short returnValue = (short)0;
    public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiForeignFutureStructU16() {
        super();
    }
    
    public UniffiForeignFutureStructU16(
        short returnValue,
        UniffiRustCallStatus.ByValue callStatus
    ) {
        this.returnValue = returnValue;
        this.callStatus = callStatus;
    }

    public static class UniffiByValue extends UniffiForeignFutureStructU16 implements Structure.ByValue {
        public UniffiByValue(
            short returnValue,
            UniffiRustCallStatus.ByValue callStatus
        ) {
            super(returnValue,        
            callStatus        
            );
        }
    }

    void uniffiSetValue(UniffiForeignFutureStructU16 other) {
        returnValue = other.returnValue;
        callStatus = other.callStatus;
    }

}
