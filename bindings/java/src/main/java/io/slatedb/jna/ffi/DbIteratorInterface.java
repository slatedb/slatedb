package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface DbIteratorInterface {

  public CompletableFuture<KeyValue> next();

  public CompletableFuture<Void> seek(byte[] key);
}
