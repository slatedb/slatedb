package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DbTransactionInterface {

  public CompletableFuture<WriteHandle> commit();

  public CompletableFuture<WriteHandle> commitWithOptions(WriteOptions options);

  public CompletableFuture<Void> delete(byte[] key);

  public CompletableFuture<byte[]> get(byte[] key);

  public CompletableFuture<KeyValue> getKeyValue(byte[] key);

  public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options);

  public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options);

  public String id();

  public CompletableFuture<Void> markRead(List<byte[]> keys);

  public CompletableFuture<Void> merge(byte[] key, byte[] operand);

  public CompletableFuture<Void> mergeWithOptions(byte[] key, byte[] operand, MergeOptions options);

  public CompletableFuture<Void> put(byte[] key, byte[] value);

  public CompletableFuture<Void> putWithOptions(byte[] key, byte[] value, PutOptions options);

  public CompletableFuture<Void> rollback();

  public CompletableFuture<DbIterator> scan(KeyRange range);

  public CompletableFuture<DbIterator> scanPrefix(byte[] prefix);

  public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options);

  public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options);

  public Long seqnum();

  public CompletableFuture<Void> unmarkWrite(List<byte[]> keys);
}
