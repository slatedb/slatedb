package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options for opening a [`crate::DbReader`].
 */
public class ReaderOptions {
    /**
     * How often the reader polls for new manifests and WAL data, in milliseconds.
     */
    private Long manifestPollIntervalMs;
    /**
     * Lifetime of an internally managed checkpoint, in milliseconds.
     */
    private Long checkpointLifetimeMs;
    /**
     * Maximum size of one in-memory table used while replaying WAL data.
     */
    private Long maxMemtableBytes;
    /**
     * Whether WAL replay should be skipped entirely.
     */
    private Boolean skipWalReplay;

    public ReaderOptions(
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
        if (other instanceof ReaderOptions) {
            ReaderOptions t = (ReaderOptions) other;
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


