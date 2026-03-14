package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface FfiDbInterface {

  public CompletableFuture<FfiDbTransaction> begin(FfiIsolationLevel isolationLevel);

  public CompletableFuture<FfiWriteHandle> delete(byte[] key);

  public CompletableFuture<FfiWriteHandle> deleteWithOptions(byte[] key, FfiWriteOptions options);

  public CompletableFuture<Void> flush();

  public CompletableFuture<Void> flushWithOptions(FfiFlushOptions options);

  public CompletableFuture<byte[]> get(byte[] key);

  public CompletableFuture<FfiKeyValue> getKeyValue(byte[] key);

  public CompletableFuture<FfiKeyValue> getKeyValueWithOptions(byte[] key, FfiReadOptions options);

  public CompletableFuture<byte[]> getWithOptions(byte[] key, FfiReadOptions options);

  public CompletableFuture<FfiWriteHandle> merge(byte[] key, byte[] operand);

  public CompletableFuture<FfiWriteHandle> mergeWithOptions(
      byte[] key, byte[] operand, FfiMergeOptions mergeOptions, FfiWriteOptions writeOptions);

  public Map<String, Long> metrics() throws FfiException;

  public CompletableFuture<FfiWriteHandle> put(byte[] key, byte[] value);

  public CompletableFuture<FfiWriteHandle> putWithOptions(
      byte[] key, byte[] value, FfiPutOptions putOptions, FfiWriteOptions writeOptions);

  public CompletableFuture<FfiDbIterator> scan(FfiKeyRange range);

  public CompletableFuture<FfiDbIterator> scanPrefix(byte[] prefix);

  public CompletableFuture<FfiDbIterator> scanPrefixWithOptions(
      byte[] prefix, FfiScanOptions options);

  public CompletableFuture<FfiDbIterator> scanWithOptions(
      FfiKeyRange range, FfiScanOptions options);

  public CompletableFuture<Void> shutdown();

  public CompletableFuture<FfiDbSnapshot> snapshot();

  public void status() throws FfiException;

  public CompletableFuture<FfiWriteHandle> write(FfiWriteBatch batch);

  public CompletableFuture<FfiWriteHandle> writeWithOptions(
      FfiWriteBatch batch, FfiWriteOptions options);
}
