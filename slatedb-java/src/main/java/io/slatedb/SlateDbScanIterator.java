package io.slatedb;

import java.util.ArrayDeque;
import java.util.List;

/// Iterator over scan results. Always close after use.
public final class SlateDbScanIterator implements AutoCloseable {
    private static final int DEFAULT_BATCH_SIZE = 64;

    private NativeInterop.IteratorHandle iterPtr;
    private boolean closed;
    private final ArrayDeque<SlateDbKeyValue> buffer = new ArrayDeque<>();
    private boolean exhausted;

    SlateDbScanIterator(NativeInterop.IteratorHandle iterPtr) {
        this.iterPtr = iterPtr;
    }

    /// Returns the next key/value pair, or `null` when the iterator is exhausted.
    ///
    /// @return Next [SlateDbKeyValue], or `null` if the scan is complete.
    public SlateDbKeyValue next() {
        if (!buffer.isEmpty()) {
            return buffer.poll();
        }
        if (exhausted) {
            return null;
        }
        List<SlateDbKeyValue> batch = NativeInterop.slatedb_iterator_next_batch(iterPtr, DEFAULT_BATCH_SIZE);
        if (batch.isEmpty()) {
            exhausted = true;
            return null;
        }
        for (SlateDbKeyValue kv : batch) {
            buffer.add(kv);
        }
        return buffer.poll();
    }

    /// Seeks to the first entry whose key is greater than or equal to the provided key.
    ///
    /// Because this iterator pre-fetches items in batches, seeking within the
    /// buffered range is handled locally without calling into the native iterator.
    /// When the seek target is beyond the buffered items, a native seek is issued.
    ///
    /// @param key key to seek to.
    public void seek(byte[] key) {
        // Drain buffered items that are before the seek target.
        while (!buffer.isEmpty()) {
            if (compareUnsigned(buffer.peek().key(), key) >= 0) {
                break;
            }
            buffer.poll();
        }

        if (!buffer.isEmpty()) {
            // Buffer has items >= seek key; the native iterator is already past
            // these items so we satisfy the seek from the buffer alone.
            return;
        }

        // Buffer is empty — the seek target is beyond all previously buffered
        // items, so we need a native seek.
        exhausted = false;
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

    /// Compares two byte arrays lexicographically as unsigned bytes,
    /// matching the key ordering used by the native iterator.
    private static int compareUnsigned(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.length - b.length;
    }
}
