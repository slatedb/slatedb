package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface FfiDbTransactionInterface {

  public CompletableFuture<FfiWriteHandle> commit();

  public CompletableFuture<FfiWriteHandle> commitWithOptions(FfiWriteOptions options);

  public CompletableFuture<Void> delete(byte[] key);

  public CompletableFuture<byte[]> get(byte[] key);

  public CompletableFuture<FfiKeyValue> getKeyValue(byte[] key);

  public CompletableFuture<FfiKeyValue> getKeyValueWithOptions(byte[] key, FfiReadOptions options);

  public CompletableFuture<byte[]> getWithOptions(byte[] key, FfiReadOptions options);

  public String id();

  public CompletableFuture<Void> markRead(List<byte[]> keys);

  public CompletableFuture<Void> merge(byte[] key, byte[] operand);

  public CompletableFuture<Void> mergeWithOptions(
      byte[] key, byte[] operand, FfiMergeOptions options);

  public CompletableFuture<Void> put(byte[] key, byte[] value);

  public CompletableFuture<Void> putWithOptions(byte[] key, byte[] value, FfiPutOptions options);

  public CompletableFuture<Void> rollback();

  public CompletableFuture<FfiDbIterator> scan(FfiKeyRange range);

  public CompletableFuture<FfiDbIterator> scanPrefix(byte[] prefix);

  public CompletableFuture<FfiDbIterator> scanPrefixWithOptions(
      byte[] prefix, FfiScanOptions options);

  public CompletableFuture<FfiDbIterator> scanWithOptions(
      FfiKeyRange range, FfiScanOptions options);

  public Long seqnum();

  public CompletableFuture<Void> unmarkWrite(List<byte[]> keys);
}
