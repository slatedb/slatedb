package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface FfiDbBuilderInterface {

  public CompletableFuture<FfiDb> build();

  public void withDbCacheDisabled() throws FfiException;

  public void withMergeOperator(FfiMergeOperator mergeOperator) throws FfiException;

  public void withSeed(Long seed) throws FfiException;

  public void withSettings(FfiSettings settings) throws FfiException;

  public void withSstBlockSize(FfiSstBlockSize sstBlockSize) throws FfiException;

  public void withWalObjectStore(FfiObjectStore walObjectStore) throws FfiException;
}
