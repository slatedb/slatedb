package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options for merge operations.
 */
public class DbMergeOptions {
    /**
     * TTL to apply to the merged value.
     */
    private Ttl ttl;

    public DbMergeOptions(
        Ttl ttl
    ) {
        
        this.ttl = ttl;
    }
    
    public Ttl ttl() {
        return this.ttl;
    }
    public void setTtl(Ttl ttl) {
        this.ttl = ttl;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof DbMergeOptions) {
            DbMergeOptions t = (DbMergeOptions) other;
            return (
              Objects.equals(ttl, t.ttl)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttl);
    }
}


