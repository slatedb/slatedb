package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options for an explicit flush request.
 */
public class FlushOptions {
    /**
     * Which storage layer should be flushed.
     */
    private FlushType flushType;

    public FlushOptions(
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
        if (other instanceof FlushOptions) {
            FlushOptions t = (FlushOptions) other;
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


