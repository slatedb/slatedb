package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * A key-value pair returned by reads and iterators.
 */
public class KeyValue {
    /**
     * The row key.
     */
    private byte[] key;
    /**
     * The row value.
     */
    private byte[] value;
    /**
     * The sequence number that produced this row.
     */
    private Long seq;
    /**
     * The creation timestamp assigned by SlateDB.
     */
    private Long createTs;
    /**
     * The optional expiry timestamp assigned by SlateDB.
     */
    private Long expireTs;

    public KeyValue(
        byte[] key, 
        byte[] value, 
        Long seq, 
        Long createTs, 
        Long expireTs
    ) {
        
        this.key = key;
        
        this.value = value;
        
        this.seq = seq;
        
        this.createTs = createTs;
        
        this.expireTs = expireTs;
    }
    
    public byte[] key() {
        return this.key;
    }
    
    public byte[] value() {
        return this.value;
    }
    
    public Long seq() {
        return this.seq;
    }
    
    public Long createTs() {
        return this.createTs;
    }
    
    public Long expireTs() {
        return this.expireTs;
    }
    public void setKey(byte[] key) {
        this.key = key;
    }
    public void setValue(byte[] value) {
        this.value = value;
    }
    public void setSeq(Long seq) {
        this.seq = seq;
    }
    public void setCreateTs(Long createTs) {
        this.createTs = createTs;
    }
    public void setExpireTs(Long expireTs) {
        this.expireTs = expireTs;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof KeyValue) {
            KeyValue t = (KeyValue) other;
            return (
              Objects.equals(key, t.key) && 
              
              Objects.equals(value, t.value) && 
              
              Objects.equals(seq, t.seq) && 
              
              Objects.equals(createTs, t.createTs) && 
              
              Objects.equals(expireTs, t.expireTs)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, seq, createTs, expireTs);
    }
}


