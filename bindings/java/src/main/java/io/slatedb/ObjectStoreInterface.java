package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A resolved object-store handle.
 *
 * Use [`resolve_object_store`] to create one of these handles and then pass it
 * into [`crate::DbBuilder`] for the main database store or the WAL store.
 */
public interface ObjectStoreInterface {
    
}

