package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options for range scans and prefix scans.
 */
public class DbScanOptions {
    /**
     * The durability level that the scan must observe.
     */
    private DurabilityLevel durabilityFilter;
    /**
     * Whether dirty state may be returned.
     */
    private Boolean dirty;
    /**
     * The number of bytes to read ahead while scanning.
     */
    private Long readAheadBytes;
    /**
     * Whether fetched blocks should be inserted into the cache.
     */
    private Boolean cacheBlocks;
    /**
     * The maximum number of background fetch tasks.
     */
    private Long maxFetchTasks;

    public DbScanOptions(
        DurabilityLevel durabilityFilter, 
        Boolean dirty, 
        Long readAheadBytes, 
        Boolean cacheBlocks, 
        Long maxFetchTasks
    ) {
        
        this.durabilityFilter = durabilityFilter;
        
        this.dirty = dirty;
        
        this.readAheadBytes = readAheadBytes;
        
        this.cacheBlocks = cacheBlocks;
        
        this.maxFetchTasks = maxFetchTasks;
    }
    
    public DurabilityLevel durabilityFilter() {
        return this.durabilityFilter;
    }
    
    public Boolean dirty() {
        return this.dirty;
    }
    
    public Long readAheadBytes() {
        return this.readAheadBytes;
    }
    
    public Boolean cacheBlocks() {
        return this.cacheBlocks;
    }
    
    public Long maxFetchTasks() {
        return this.maxFetchTasks;
    }
    public void setDurabilityFilter(DurabilityLevel durabilityFilter) {
        this.durabilityFilter = durabilityFilter;
    }
    public void setDirty(Boolean dirty) {
        this.dirty = dirty;
    }
    public void setReadAheadBytes(Long readAheadBytes) {
        this.readAheadBytes = readAheadBytes;
    }
    public void setCacheBlocks(Boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }
    public void setMaxFetchTasks(Long maxFetchTasks) {
        this.maxFetchTasks = maxFetchTasks;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof DbScanOptions) {
            DbScanOptions t = (DbScanOptions) other;
            return (
              Objects.equals(durabilityFilter, t.durabilityFilter) && 
              
              Objects.equals(dirty, t.dirty) && 
              
              Objects.equals(readAheadBytes, t.readAheadBytes) && 
              
              Objects.equals(cacheBlocks, t.cacheBlocks) && 
              
              Objects.equals(maxFetchTasks, t.maxFetchTasks)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(durabilityFilter, dirty, readAheadBytes, cacheBlocks, maxFetchTasks);
    }
}


