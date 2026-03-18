package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface DbBuilderInterface {

  public CompletableFuture<Db> build();

  public void withDbCacheDisabled() throws DbException;

  public void withMergeOperator(MergeOperator mergeOperator) throws DbException;

  public void withSeed(Long seed) throws DbException;

  public void withSettings(Settings settings) throws DbException;

  public void withSstBlockSize(SstBlockSize sstBlockSize) throws DbException;

  public void withWalObjectStore(ObjectStore walObjectStore) throws DbException;
}
