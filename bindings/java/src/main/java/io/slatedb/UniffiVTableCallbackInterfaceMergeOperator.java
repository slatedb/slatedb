package io.slatedb;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

@Structure.FieldOrder({ "merge", "uniffiFree" })
public class UniffiVTableCallbackInterfaceMergeOperator extends Structure {
    public UniffiCallbackInterfaceMergeOperatorMethod0 merge = null;
    public UniffiCallbackInterfaceFree uniffiFree = null;

    // no-arg constructor required so JNA can instantiate and reflect
    public UniffiVTableCallbackInterfaceMergeOperator() {
        super();
    }
    
    public UniffiVTableCallbackInterfaceMergeOperator(
        UniffiCallbackInterfaceMergeOperatorMethod0 merge,
        UniffiCallbackInterfaceFree uniffiFree
    ) {
        this.merge = merge;
        this.uniffiFree = uniffiFree;
    }

    public static class UniffiByValue extends UniffiVTableCallbackInterfaceMergeOperator implements Structure.ByValue {
        public UniffiByValue(
            UniffiCallbackInterfaceMergeOperatorMethod0 merge,
            UniffiCallbackInterfaceFree uniffiFree
        ) {
            super(merge,        
            uniffiFree        
            );
        }
    }

    void uniffiSetValue(UniffiVTableCallbackInterfaceMergeOperator other) {
        merge = other.merge;
        uniffiFree = other.uniffiFree;
    }

}




































































































































































































