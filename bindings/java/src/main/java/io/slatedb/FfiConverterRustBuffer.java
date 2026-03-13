package io.slatedb;


// FfiConverter that uses `RustBuffer` as the FfiType
public interface FfiConverterRustBuffer<JavaType> extends FfiConverter<JavaType, RustBuffer.ByValue> {
    @Override
    default JavaType lift(RustBuffer.ByValue value) {
        return liftFromRustBuffer(value);
    }
    @Override
    default RustBuffer.ByValue lower(JavaType value) {
        return lowerIntoRustBuffer(value);
    }
}
