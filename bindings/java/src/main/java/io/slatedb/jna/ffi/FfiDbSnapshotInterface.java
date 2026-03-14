package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface FfiDbSnapshotInterface {

  public CompletableFuture<byte[]> get(byte[] key);

  public CompletableFuture<FfiKeyValue> getKeyValue(byte[] key);

  public CompletableFuture<FfiKeyValue> getKeyValueWithOptions(byte[] key, FfiReadOptions options);

  public CompletableFuture<byte[]> getWithOptions(byte[] key, FfiReadOptions options);

  public CompletableFuture<FfiDbIterator> scan(FfiKeyRange range);

  public CompletableFuture<FfiDbIterator> scanPrefix(byte[] prefix);

  public CompletableFuture<FfiDbIterator> scanPrefixWithOptions(
      byte[] prefix, FfiScanOptions options);

  public CompletableFuture<FfiDbIterator> scanWithOptions(
      FfiKeyRange range, FfiScanOptions options);
}
