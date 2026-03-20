package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "returnValue", "callStatus" })
public class UniffiForeignFutureStructU32 extends Structure {
    public int returnValue = 0;
    public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiForeignFutureStructU32() {
        super();
    }
    
    public UniffiForeignFutureStructU32(
        int returnValue,
        UniffiRustCallStatus.ByValue callStatus
    ) {
        this.returnValue = returnValue;
        this.callStatus = callStatus;
    }

    public static class UniffiByValue extends UniffiForeignFutureStructU32 implements Structure.ByValue {
        public UniffiByValue(
            int returnValue,
            UniffiRustCallStatus.ByValue callStatus
        ) {
            super(returnValue,        
            callStatus        
            );
        }
    }

    void uniffiSetValue(UniffiForeignFutureStructU32 other) {
        returnValue = other.returnValue;
        callStatus = other.callStatus;
    }

}
