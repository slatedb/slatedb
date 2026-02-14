package io.slatedb;

/// Iterator over scan results. Always close after use.
public final class SlateDbScanIterator implements AutoCloseable {
    private NativeInterop.IteratorHandle iterPtr;
    private boolean closed;

    SlateDbScanIterator(NativeInterop.IteratorHandle iterPtr) {
        this.iterPtr = iterPtr;
    }

    /// Returns the next key/value pair, or `null` when the iterator is exhausted.
    ///
    /// @return Next [SlateDbKeyValue], or `null` if the scan is complete.
    public SlateDbKeyValue next() {
        NativeInterop.IteratorNextResult next = NativeInterop.slatedb_iterator_next(iterPtr);
        if (!next.present()) {
            return null;
        }
        return new SlateDbKeyValue(next.key(), next.value());
    }

    /// Seeks to the first entry whose key is greater than or equal to the provided key.
    ///
    /// @param key key to seek to.
    public void seek(byte[] key) {
        NativeInterop.slatedb_iterator_seek(iterPtr, key);
    }

    /// Closes the iterator and releases native resources.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        NativeInterop.slatedb_iterator_close(iterPtr);
        iterPtr = null;
        closed = true;
    }
}
