package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.CompletableFuture;

public interface DbReaderInterface {

  public CompletableFuture<byte[]> get(byte[] key);

  public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options);

  public CompletableFuture<DbIterator> scan(KeyRange range);

  public CompletableFuture<DbIterator> scanPrefix(byte[] prefix);

  public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options);

  public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options);

  public CompletableFuture<Void> shutdown();
}
