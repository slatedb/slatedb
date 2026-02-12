package io.slatedb;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/// Iterator over scan results. Always close after use.
public final class SlateDbScanIterator implements AutoCloseable {
    private MemorySegment iterPtr;
    private boolean closed;

    SlateDbScanIterator(MemorySegment iterPtr) {
        this.iterPtr = iterPtr;
    }

    /// Returns the next key/value pair, or `null` when the iterator is exhausted.
    ///
    /// @return Next [SlateDbKeyValue], or `null` if the scan is complete.
    public SlateDbKeyValue next() {
        return Native.iteratorNext(iterPtr);
    }

    /// Seeks to the first entry whose key is greater than or equal to the provided key.
    ///
    /// @param key key to seek to.
    public void seek(final ByteBuffer key) {
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
}
