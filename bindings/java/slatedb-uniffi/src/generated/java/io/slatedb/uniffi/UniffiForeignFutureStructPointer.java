package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "returnValue", "callStatus" })
public class UniffiForeignFutureStructPointer extends Structure {
    public Pointer returnValue = Pointer.NULL;
    public UniffiRustCallStatus.ByValue callStatus = new UniffiRustCallStatus.ByValue();

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiForeignFutureStructPointer() {
        super();
    }
    
    public UniffiForeignFutureStructPointer(
        Pointer returnValue,
        UniffiRustCallStatus.ByValue callStatus
    ) {
        this.returnValue = returnValue;
        this.callStatus = callStatus;
    }

    public static class UniffiByValue extends UniffiForeignFutureStructPointer implements Structure.ByValue {
        public UniffiByValue(
            Pointer returnValue,
            UniffiRustCallStatus.ByValue callStatus
        ) {
            super(returnValue,        
            callStatus        
            );
        }
    }

    void uniffiSetValue(UniffiForeignFutureStructPointer other) {
        returnValue = other.returnValue;
        callStatus = other.callStatus;
    }

}
