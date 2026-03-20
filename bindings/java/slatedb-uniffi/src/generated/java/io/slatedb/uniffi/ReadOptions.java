package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options that control a point read.
 */
public class ReadOptions {
    /**
     * Minimum durability level a returned row must satisfy.
     */
    private DurabilityLevel durabilityFilter;
    /**
     * Whether uncommitted dirty data may be returned.
     */
    private Boolean dirty;
    /**
     * Whether fetched blocks should be inserted into the block cache.
     */
    private Boolean cacheBlocks;

    public ReadOptions(
        DurabilityLevel durabilityFilter, 
        Boolean dirty, 
        Boolean cacheBlocks
    ) {
        
        this.durabilityFilter = durabilityFilter;
        
        this.dirty = dirty;
        
        this.cacheBlocks = cacheBlocks;
    }
    
    public DurabilityLevel durabilityFilter() {
        return this.durabilityFilter;
    }
    
    public Boolean dirty() {
        return this.dirty;
    }
    
    public Boolean cacheBlocks() {
        return this.cacheBlocks;
    }
    public void setDurabilityFilter(DurabilityLevel durabilityFilter) {
        this.durabilityFilter = durabilityFilter;
    }
    public void setDirty(Boolean dirty) {
        this.dirty = dirty;
    }
    public void setCacheBlocks(Boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof ReadOptions) {
            ReadOptions t = (ReadOptions) other;
            return (
              Objects.equals(durabilityFilter, t.durabilityFilter) && 
              
              Objects.equals(dirty, t.dirty) && 
              
              Objects.equals(cacheBlocks, t.cacheBlocks)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(durabilityFilter, dirty, cacheBlocks);
    }
}


