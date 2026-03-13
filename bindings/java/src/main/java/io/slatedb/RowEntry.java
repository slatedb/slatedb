package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * A row entry returned by WAL iteration.
 */
public class RowEntry {
    private RowEntryKind kind;
    private byte[] key;
    private byte[] value;
    private Long seq;
    private Long createTs;
    private Long expireTs;

    public RowEntry(
        RowEntryKind kind, 
        byte[] key, 
        byte[] value, 
        Long seq, 
        Long createTs, 
        Long expireTs
    ) {
        
        this.kind = kind;
        
        this.key = key;
        
        this.value = value;
        
        this.seq = seq;
        
        this.createTs = createTs;
        
        this.expireTs = expireTs;
    }
    
    public RowEntryKind kind() {
        return this.kind;
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
    public void setKind(RowEntryKind kind) {
        this.kind = kind;
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
        if (other instanceof RowEntry) {
            RowEntry t = (RowEntry) other;
            return (
              Objects.equals(kind, t.kind) && 
              
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
        return Objects.hash(kind, key, value, seq, createTs, expireTs);
    }
}


