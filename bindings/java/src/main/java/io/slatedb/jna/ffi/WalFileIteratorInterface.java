package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface WalFileIteratorInterface {

  public CompletableFuture<RowEntry> next();

  public void shutdown() throws DbException;
}
