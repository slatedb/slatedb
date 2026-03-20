package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options applied to a put operation.
 */
public class PutOptions {
    /**
     * TTL policy for the inserted value.
     */
    private Ttl ttl;

    public PutOptions(
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
        if (other instanceof PutOptions) {
            PutOptions t = (PutOptions) other;
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


