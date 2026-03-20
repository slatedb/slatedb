package io.slatedb.uniffi;


// Java doesn't allow for static init blocks in an interface outside of a static property with a default.
// To get around that and make sure that when the UniffiLib interface loads it has an initialized library
// we call this class. The init code won't be called until a function on this interface is called unfortunately.
final class UniffiLibInitializer {
    static UniffiLib load() {
        UniffiLib instance = NamespaceLibrary.loadIndirect("slatedb", UniffiLib.class);
        NamespaceLibrary.uniffiCheckContractApiVersion(instance);
        NamespaceLibrary.uniffiCheckApiChecksums(instance);
        UniffiCallbackInterfaceLogCallback.INSTANCE.register(instance);
        UniffiCallbackInterfaceMergeOperator.INSTANCE.register(instance);
        return instance;
    }
}

// Async support
