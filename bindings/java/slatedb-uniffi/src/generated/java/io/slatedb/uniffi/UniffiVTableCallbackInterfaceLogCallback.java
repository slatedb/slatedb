package io.slatedb.uniffi;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "log", "uniffiFree" })
public class UniffiVTableCallbackInterfaceLogCallback extends Structure {
    public UniffiCallbackInterfaceLogCallbackMethod0 log = null;
    public UniffiCallbackInterfaceFree uniffiFree = null;

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiVTableCallbackInterfaceLogCallback() {
        super();
    }
    
    public UniffiVTableCallbackInterfaceLogCallback(
        UniffiCallbackInterfaceLogCallbackMethod0 log,
        UniffiCallbackInterfaceFree uniffiFree
    ) {
        this.log = log;
        this.uniffiFree = uniffiFree;
    }

    public static class UniffiByValue extends UniffiVTableCallbackInterfaceLogCallback implements Structure.ByValue {
        public UniffiByValue(
            UniffiCallbackInterfaceLogCallbackMethod0 log,
            UniffiCallbackInterfaceFree uniffiFree
        ) {
            super(log,        
            uniffiFree        
            );
        }
    }

    void uniffiSetValue(UniffiVTableCallbackInterfaceLogCallback other) {
        log = other.log;
        uniffiFree = other.uniffiFree;
    }

}
