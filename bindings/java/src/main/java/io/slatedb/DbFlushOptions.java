package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options for manual flushes.
 */
public class DbFlushOptions {
    /**
     * The flush mode to execute.
     */
    private FlushType flushType;

    public DbFlushOptions(
        FlushType flushType
    ) {
        
        this.flushType = flushType;
    }
    
    public FlushType flushType() {
        return this.flushType;
    }
    public void setFlushType(FlushType flushType) {
        this.flushType = flushType;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof DbFlushOptions) {
            DbFlushOptions t = (DbFlushOptions) other;
            return (
              Objects.equals(flushType, t.flushType)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(flushType);
    }
}


