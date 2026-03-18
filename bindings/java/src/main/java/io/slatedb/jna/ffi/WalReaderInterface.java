package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface WalReaderInterface {

  public WalFile get(Long id);

  public CompletableFuture<List<WalFile>> list(Long startId, Long endId);

  public void shutdown() throws DbException;
}
