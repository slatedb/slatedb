package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface WalFileInterface {

  public Long id();

  public CompletableFuture<WalFileIterator> iterator();

  public CompletableFuture<WalFileMetadata> metadata();

  public WalFile nextFile();

  public Long nextId();

  public void shutdown() throws DbException;
}
