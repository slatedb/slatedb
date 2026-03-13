package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options for constructing a read-only database reader.
 */
public class DbReaderOptions {
    /**
     * How often to poll manifests and WALs for refreshed reader state.
     */
    private Long manifestPollIntervalMs;
    /**
     * How long reader-owned checkpoints should remain valid.
     */
    private Long checkpointLifetimeMs;
    /**
     * Maximum WAL replay memtable size in bytes.
     */
    private Long maxMemtableBytes;
    /**
     * Whether WAL replay should be skipped entirely.
     */
    private Boolean skipWalReplay;

    public DbReaderOptions(
        Long manifestPollIntervalMs, 
        Long checkpointLifetimeMs, 
        Long maxMemtableBytes, 
        Boolean skipWalReplay
    ) {
        
        this.manifestPollIntervalMs = manifestPollIntervalMs;
        
        this.checkpointLifetimeMs = checkpointLifetimeMs;
        
        this.maxMemtableBytes = maxMemtableBytes;
        
        this.skipWalReplay = skipWalReplay;
    }
    
    public Long manifestPollIntervalMs() {
        return this.manifestPollIntervalMs;
    }
    
    public Long checkpointLifetimeMs() {
        return this.checkpointLifetimeMs;
    }
    
    public Long maxMemtableBytes() {
        return this.maxMemtableBytes;
    }
    
    public Boolean skipWalReplay() {
        return this.skipWalReplay;
    }
    public void setManifestPollIntervalMs(Long manifestPollIntervalMs) {
        this.manifestPollIntervalMs = manifestPollIntervalMs;
    }
    public void setCheckpointLifetimeMs(Long checkpointLifetimeMs) {
        this.checkpointLifetimeMs = checkpointLifetimeMs;
    }
    public void setMaxMemtableBytes(Long maxMemtableBytes) {
        this.maxMemtableBytes = maxMemtableBytes;
    }
    public void setSkipWalReplay(Boolean skipWalReplay) {
        this.skipWalReplay = skipWalReplay;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof DbReaderOptions) {
            DbReaderOptions t = (DbReaderOptions) other;
            return (
              Objects.equals(manifestPollIntervalMs, t.manifestPollIntervalMs) && 
              
              Objects.equals(checkpointLifetimeMs, t.checkpointLifetimeMs) && 
              
              Objects.equals(maxMemtableBytes, t.maxMemtableBytes) && 
              
              Objects.equals(skipWalReplay, t.skipWalReplay)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestPollIntervalMs, checkpointLifetimeMs, maxMemtableBytes, skipWalReplay);
    }
}


