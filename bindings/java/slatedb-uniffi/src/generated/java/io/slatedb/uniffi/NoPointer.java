package io.slatedb.uniffi;


public class NoPointer {
    // Private constructor to prevent instantiation
    private NoPointer() {}

    // Static final instance of the class so it can be used in tests
    public static final NoPointer INSTANCE = new NoPointer();
}

