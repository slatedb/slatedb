package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface DbReaderBuilderInterface {

  public CompletableFuture<DbReader> build();

  public void withCheckpointId(String checkpointId) throws DbException;

  public void withMergeOperator(MergeOperator mergeOperator) throws DbException;

  public void withOptions(ReaderOptions options) throws DbException;

  public void withWalObjectStore(ObjectStore walObjectStore) throws DbException;
}
