package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Objects;
/**
 * Options that control durability behavior for writes and commits.
 */
public class WriteOptions {
    /**
     * Whether the call waits for the write to become durable before returning.
     */
    private Boolean awaitDurable;

    public WriteOptions(
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
        if (other instanceof WriteOptions) {
            WriteOptions t = (WriteOptions) other;
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


