package io.slatedb;

import java.util.ArrayList;
import java.util.List;

/// Read-only WAL reader for inspecting raw WAL files in object storage.
///
/// WAL readers provide direct access to Write-Ahead Log files and should be closed when done.
public final class SlateDbWalReader implements AutoCloseable {
    private NativeInterop.WalReaderHandle handle;
    private boolean closed;

    private SlateDbWalReader(NativeInterop.WalReaderHandle handle) {
        this.handle = handle;
    }

    /// Opens a WAL reader for the given database path and object store.
    ///
    /// @param path    database path within the object store.
    /// @param url     object store URL, or {@code null} to resolve from the environment.
    /// @param envFile path to a .env file to load into the environment, or {@code null}.
    /// @return A new [SlateDbWalReader]. Always close it.
    public static SlateDbWalReader open(String path, String url, String envFile) {
        try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.resolveObjectStore(url, envFile)) {
            return new SlateDbWalReader(NativeInterop.slatedb_wal_reader_new(path, objectStore));
        }
    }

    /// Lists WAL files, optionally filtered by ID range.
    ///
    /// @param startId inclusive lower bound on WAL file ID, or {@code null} for no lower bound.
    /// @param endId   exclusive upper bound on WAL file ID, or {@code null} for no upper bound.
    /// @return A list of [SlateDbWalFile] handles. Always close each one.
    public List<SlateDbWalFile> list(Long startId, Long endId) {
        List<NativeInterop.WalFileHandle> handles = NativeInterop.slatedb_wal_reader_list(handle, startId, endId);
        List<SlateDbWalFile> files = new ArrayList<>(handles.size());
        for (NativeInterop.WalFileHandle h : handles) {
            files.add(new SlateDbWalFile(h));
        }
        return files;
    }

    /// Gets a specific WAL file by ID.
    ///
    /// @param id WAL file ID.
    /// @return A [SlateDbWalFile] handle. Always close it.
    public SlateDbWalFile get(long id) {
        return new SlateDbWalFile(NativeInterop.slatedb_wal_reader_get(handle, id));
    }

    /// Closes the WAL reader handle.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        NativeInterop.slatedb_wal_reader_close(handle);
        handle = null;
        closed = true;
    }
}
