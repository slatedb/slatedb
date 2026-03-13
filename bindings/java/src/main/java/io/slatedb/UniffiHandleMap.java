package io.slatedb;


import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

// This is used pass an opaque 64-bit handle representing a foreign object to the Rust code.
class UniffiHandleMap<T extends Object> {
    private final ConcurrentHashMap<Long, T> map = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(0);

    public int size() {
        return map.size();
    }

    // Insert a new object into the handle map and get a handle for it
    public long insert(T obj) {
        long handle = counter.getAndAdd(1);
        map.put(handle, obj);
        return handle;
    }

    // Get an object from the handle map
    public T get(long handle) {
        T obj = map.get(handle);
        if (obj == null) {
            throw new InternalException("UniffiHandleMap.get: Invalid handle");
        }
        return obj;
    }

    // Remove an entry from the handlemap and get the Java object back
    public T remove(long handle) {
        T obj = map.remove(handle);
        if (obj == null) {
            throw new InternalException("UniffiHandleMap: Invalid handle");
        }
        return obj;
    }
}

// Contains loading, initialization code,
// and the FFI Function declarations in a com.sun.jna.Library.
