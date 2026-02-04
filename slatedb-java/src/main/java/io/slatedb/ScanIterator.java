package io.slatedb;

import java.lang.foreign.MemorySegment;

/// Iterator over scan results. Always close after use.
public final class ScanIterator implements AutoCloseable {
    private MemorySegment iterPtr;
    private boolean closed;

    ScanIterator(MemorySegment iterPtr) {
        this.iterPtr = iterPtr;
    }

    /// Returns the next key/value pair, or `null` when the iterator is exhausted.
    ///
    /// @return Next [KeyValue], or `null` if the scan is complete.
    public KeyValue next() {
        ensureOpen();
        return Native.iteratorNext(iterPtr);
    }

    /// Seeks to the first entry whose key is greater than or equal to the provided key.
    ///
    /// @param key key to seek to.
    public void seek(byte[] key) {
        ensureOpen();
        Native.iteratorSeek(iterPtr, key);
    }

    /// Closes the iterator and releases native resources.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        Native.iteratorClose(iterPtr);
        iterPtr = MemorySegment.NULL;
        closed = true;
    }

    private void ensureOpen() {
        if (closed || iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Iterator is closed");
        }
    }
}
