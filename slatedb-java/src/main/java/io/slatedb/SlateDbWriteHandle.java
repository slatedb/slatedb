package io.slatedb;

import java.util.OptionalLong;

/**
 * Handle returned by SlateDB write operations.
 */
public class SlateDbWriteHandle {
    private final long seq;
    private final OptionalLong createTs;

    public SlateDbWriteHandle(long seq, OptionalLong createTs) {
        this.seq = seq;
        this.createTs = createTs;
    }

    /**
     * @return The sequence number of the write operation.
     */
    public long seq() {
        return seq;
    }

    /**
     * @return The creation timestamp of the write operation, or empty if not available.
     */
    public OptionalLong createTs() {
        return createTs;
    }
}
