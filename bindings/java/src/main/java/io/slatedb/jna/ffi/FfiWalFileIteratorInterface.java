package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface FfiWalFileIteratorInterface {

  public CompletableFuture<FfiRowEntry> next();

  public void shutdown() throws FfiException;
}
