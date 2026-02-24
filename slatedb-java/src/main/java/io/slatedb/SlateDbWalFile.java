package io.slatedb;

/// Handle to a single WAL file. Always close after use.
public final class SlateDbWalFile implements AutoCloseable {
    private NativeInterop.WalFileHandle handle;
    private boolean closed;

    SlateDbWalFile(NativeInterop.WalFileHandle handle) {
        this.handle = handle;
    }

    /// Returns the WAL file ID.
    public long id() {
        return NativeInterop.slatedb_wal_file_id(handle);
    }

    /// Returns the ID of the next WAL file.
    public long nextId() {
        return NativeInterop.slatedb_wal_file_next_id(handle);
    }

    /// Opens the next WAL file in the sequence.
    ///
    /// @return The next [SlateDbWalFile]. Always close it.
    public SlateDbWalFile nextFile() {
        return new SlateDbWalFile(NativeInterop.slatedb_wal_file_next_file(handle));
    }

    /// Returns metadata for this WAL file.
    public SlateDbWalFileMetadata metadata() {
        return NativeInterop.slatedb_wal_file_metadata(handle);
    }

    /// Creates an iterator over the entries in this WAL file.
    ///
    /// @return A [SlateDbWalFileIterator]. Always close it.
    public SlateDbWalFileIterator iterator() {
        return new SlateDbWalFileIterator(NativeInterop.slatedb_wal_file_iterator(handle));
    }

    /// Closes the WAL file handle.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        NativeInterop.slatedb_wal_file_close(handle);
        handle = null;
        closed = true;
    }
}
