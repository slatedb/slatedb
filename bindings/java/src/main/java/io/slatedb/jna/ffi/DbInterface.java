package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface DbInterface {

  public CompletableFuture<DbTransaction> begin(IsolationLevel isolationLevel);

  public CompletableFuture<WriteHandle> delete(byte[] key);

  public CompletableFuture<WriteHandle> deleteWithOptions(byte[] key, WriteOptions options);

  public CompletableFuture<Void> flush();

  public CompletableFuture<Void> flushWithOptions(FlushOptions options);

  public CompletableFuture<byte[]> get(byte[] key);

  public CompletableFuture<KeyValue> getKeyValue(byte[] key);

  public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options);

  public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options);

  public CompletableFuture<WriteHandle> merge(byte[] key, byte[] operand);

  public CompletableFuture<WriteHandle> mergeWithOptions(
      byte[] key, byte[] operand, MergeOptions mergeOptions, WriteOptions writeOptions);

  public Map<String, Long> metrics() throws DbException;

  public CompletableFuture<WriteHandle> put(byte[] key, byte[] value);

  public CompletableFuture<WriteHandle> putWithOptions(
      byte[] key, byte[] value, PutOptions putOptions, WriteOptions writeOptions);

  public CompletableFuture<DbIterator> scan(KeyRange range);

  public CompletableFuture<DbIterator> scanPrefix(byte[] prefix);

  public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options);

  public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options);

  public CompletableFuture<Void> shutdown();

  public CompletableFuture<DbSnapshot> snapshot();

  public void status() throws DbException;

  public CompletableFuture<WriteHandle> write(WriteBatch batch);

  public CompletableFuture<WriteHandle> writeWithOptions(WriteBatch batch, WriteOptions options);
}
