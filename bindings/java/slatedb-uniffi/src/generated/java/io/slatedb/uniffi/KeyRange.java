package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * A half-open or closed byte-key range used by scan APIs.
 */
public class KeyRange {
    /**
     * Inclusive or exclusive lower bound. `None` means unbounded.
     */
    private byte[] start;
    /**
     * Whether `start` is inclusive when present.
     */
    private Boolean startInclusive;
    /**
     * Inclusive or exclusive upper bound. `None` means unbounded.
     */
    private byte[] end;
    /**
     * Whether `end` is inclusive when present.
     */
    private Boolean endInclusive;

    public KeyRange(
        byte[] start, 
        Boolean startInclusive, 
        byte[] end, 
        Boolean endInclusive
    ) {
        
        this.start = start;
        
        this.startInclusive = startInclusive;
        
        this.end = end;
        
        this.endInclusive = endInclusive;
    }
    
    public byte[] start() {
        return this.start;
    }
    
    public Boolean startInclusive() {
        return this.startInclusive;
    }
    
    public byte[] end() {
        return this.end;
    }
    
    public Boolean endInclusive() {
        return this.endInclusive;
    }
    public void setStart(byte[] start) {
        this.start = start;
    }
    public void setStartInclusive(Boolean startInclusive) {
        this.startInclusive = startInclusive;
    }
    public void setEnd(byte[] end) {
        this.end = end;
    }
    public void setEndInclusive(Boolean endInclusive) {
        this.endInclusive = endInclusive;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof KeyRange) {
            KeyRange t = (KeyRange) other;
            return (
              Objects.equals(start, t.start) && 
              
              Objects.equals(startInclusive, t.startInclusive) && 
              
              Objects.equals(end, t.end) && 
              
              Objects.equals(endInclusive, t.endInclusive)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, startInclusive, end, endInclusive);
    }
}


