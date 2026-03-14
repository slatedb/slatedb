package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface FfiWalFileInterface {

  public Long id();

  public CompletableFuture<FfiWalFileIterator> iterator();

  public CompletableFuture<FfiWalFileMetadata> metadata();

  public FfiWalFile nextFile();

  public Long nextId();

  public void shutdown() throws FfiException;
}
