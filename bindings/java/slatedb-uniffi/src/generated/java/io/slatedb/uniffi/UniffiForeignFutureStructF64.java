package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "returnValue", "callStatus" })
public class UniffiForeignFutureStructF64 extends Structure {
    public double returnValue = 0.0;
    public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiForeignFutureStructF64() {
        super();
    }
    
    public UniffiForeignFutureStructF64(
        double returnValue,
        UniffiRustCallStatus.ByValue callStatus
    ) {
        this.returnValue = returnValue;
        this.callStatus = callStatus;
    }

    public static class UniffiByValue extends UniffiForeignFutureStructF64 implements Structure.ByValue {
        public UniffiByValue(
            double returnValue,
            UniffiRustCallStatus.ByValue callStatus
        ) {
            super(returnValue,        
            callStatus        
            );
        }
    }

    void uniffiSetValue(UniffiForeignFutureStructF64 other) {
        returnValue = other.returnValue;
        callStatus = other.callStatus;
    }

}
