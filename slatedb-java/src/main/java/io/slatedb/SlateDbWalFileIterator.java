package io.slatedb;

/// Iterator over WAL entries in a single WAL file. Always close after use.
public final class SlateDbWalFileIterator implements AutoCloseable {
    private NativeInterop.WalFileIteratorHandle handle;
    private boolean closed;

    SlateDbWalFileIterator(NativeInterop.WalFileIteratorHandle handle) {
        this.handle = handle;
    }

    /// Returns the next row entry, or {@code null} when the iterator is exhausted.
    ///
    /// @return Next [SlateDbRowEntry], or {@code null} if the iterator is done.
    public SlateDbRowEntry next() {
        NativeInterop.WalIteratorNextResult result = NativeInterop.slatedb_wal_file_iterator_next(handle);
        if (!result.present()) {
            return null;
        }
        SlateDbConfig.RowEntryKind kind = SlateDbConfig.RowEntryKind.fromCode(result.kind());
        return new SlateDbRowEntry(kind, result.key(), result.value(), result.seq(), result.createTs(), result.expireTs());
    }

    /// Closes the iterator and releases native resources.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        NativeInterop.slatedb_wal_file_iterator_close(handle);
        handle = null;
        closed = true;
    }
}
