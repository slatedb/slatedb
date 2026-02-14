package io.slatedb;

/**
 * Handle returned by SlateDB write operations.
 */
public class SlateDbWriteHandle {
    private final long seq;
    private final Long createTs;

    public SlateDbWriteHandle(long seq, Long createTs) {
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
     * @return The creation timestamp of the write operation, or null if not available.
     */
    public Long createTs() {
        return createTs;
    }
}
