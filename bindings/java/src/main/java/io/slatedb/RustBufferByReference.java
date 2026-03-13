package io.slatedb;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;
/**
 * The equivalent of the `*mut RustBuffer` type.
 * Required for callbacks taking in an out pointer.
 *
 * Size is the sum of all values in the struct.
 */
public class RustBufferByReference extends Structure implements Structure.ByReference {
    public RustBufferByReference() {
        super(16);
    }

    /**
     * Set the pointed-to `RustBuffer` to the given value.
     */
    public void setValue(RustBuffer.ByValue value) {
        // NOTE: The offsets are as they are in the C-like struct.
        Pointer pointer = this.getPointer();
        pointer.setInt(0, (int) value.capacity);
        pointer.setInt(4, (int) value.len);
        pointer.setPointer(8, value.data);
    }

    /**
     * Get a `RustBuffer.ByValue` from this reference.
     */
    public RustBuffer.ByValue getValue() {
        Pointer pointer = this.getPointer();
        RustBuffer.ByValue value = new RustBuffer.ByValue();
        value.writeField("capacity", pointer.getLong(0));
        value.writeField("len", pointer.getLong(8));
        value.writeField("data", pointer.getLong(16));

        return value;
    }
}

