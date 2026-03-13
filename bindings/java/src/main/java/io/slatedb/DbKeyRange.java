package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * A range of keys used for scans.
 */
public class DbKeyRange {
    /**
     * The optional lower bound of the range.
     */
    private byte[] start;
    /**
     * Whether the lower bound is inclusive.
     */
    private Boolean startInclusive;
    /**
     * The optional upper bound of the range.
     */
    private byte[] end;
    /**
     * Whether the upper bound is inclusive.
     */
    private Boolean endInclusive;

    public DbKeyRange(
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
        if (other instanceof DbKeyRange) {
            DbKeyRange t = (DbKeyRange) other;
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


