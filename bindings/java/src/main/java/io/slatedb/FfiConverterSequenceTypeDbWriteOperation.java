package io.slatedb;


import java.util.List;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public enum FfiConverterSequenceTypeDbWriteOperation implements FfiConverterRustBuffer<List<DbWriteOperation>> {
  INSTANCE;

  @Override
  public List<DbWriteOperation> read(ByteBuffer buf) {
    int len = buf.getInt();
    return IntStream.range(0, len).mapToObj(_i -> FfiConverterTypeDbWriteOperation.INSTANCE.read(buf)).toList();
  }

  @Override
  public long allocationSize(List<DbWriteOperation> value) {
    long sizeForLength = 4L;
    long sizeForItems = value.stream().mapToLong(inner -> FfiConverterTypeDbWriteOperation.INSTANCE.allocationSize(inner)).sum();
    return sizeForLength + sizeForItems;
  }

  @Override
  public void write(List<DbWriteOperation> value, ByteBuffer buf) {
    buf.putInt(value.size());
    value.forEach(inner -> FfiConverterTypeDbWriteOperation.INSTANCE.write(inner, buf));
  }
}

