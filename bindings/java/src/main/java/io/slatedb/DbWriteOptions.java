package io.slatedb;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options that control write durability.
 */
public class DbWriteOptions {
    /**
     * Whether the call should wait for the write to become durable.
     */
    private Boolean awaitDurable;

    public DbWriteOptions(
        Boolean awaitDurable
    ) {
        
        this.awaitDurable = awaitDurable;
    }
    
    public Boolean awaitDurable() {
        return this.awaitDurable;
    }
    public void setAwaitDurable(Boolean awaitDurable) {
        this.awaitDurable = awaitDurable;
    }

    
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof DbWriteOptions) {
            DbWriteOptions t = (DbWriteOptions) other;
            return (
              Objects.equals(awaitDurable, t.awaitDurable)
              
            );
        };
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(awaitDurable);
    }
}


