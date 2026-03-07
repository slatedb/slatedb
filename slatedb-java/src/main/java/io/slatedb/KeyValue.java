package io.slatedb;

import java.time.Instant;

/**
 * Represents a key-value pair with metadata from the database.
 */
public class KeyValue {
    private final byte[] key;
    private final byte[] value;
    private final long seq;
    private final long createTs;
    private final Long expireTs;

    public KeyValue(byte[] key, byte[] value, long seq, long createTs, Long expireTs) {
        this.key = key;
        this.value = value;
        this.seq = seq;
        this.createTs = createTs;
        this.expireTs = expireTs;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public long seq() {
        return seq;
    }

    public Instant createTs() {
        return Instant.ofEpochMilli(createTs);
    }

    public Instant expireTs() {
        return expireTs == null ? null : Instant.ofEpochMilli(expireTs);
    }
}
