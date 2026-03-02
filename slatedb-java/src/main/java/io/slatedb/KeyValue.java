package io.slatedb;

import java.time.Instant;
import java.util.Optional;

/**
 * Represents a key-value pair with metadata from the database.
 */
public class KeyValue {
    private final byte[] key;
    private final byte[] value;
    private final long seq;
    private final Long createTs;
    private final Long expireTs;

    public KeyValue(byte[] key, byte[] value, long seq, Long createTs, Long expireTs) {
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

    public Optional<Instant> createTs() {
        return createTs != null ? Optional.of(Instant.ofEpochMilli(createTs)) : Optional.empty();
    }

    public Optional<Instant> expireTs() {
        return expireTs != null ? Optional.of(Instant.ofEpochMilli(expireTs)) : Optional.empty();
    }
}
