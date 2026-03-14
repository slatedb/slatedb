package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface FfiDbReaderBuilderInterface {

  public CompletableFuture<FfiDbReader> build();

  public void withCheckpointId(String checkpointId) throws FfiException;

  public void withMergeOperator(FfiMergeOperator mergeOperator) throws FfiException;

  public void withOptions(FfiReaderOptions options) throws FfiException;

  public void withWalObjectStore(FfiObjectStore walObjectStore) throws FfiException;
}
