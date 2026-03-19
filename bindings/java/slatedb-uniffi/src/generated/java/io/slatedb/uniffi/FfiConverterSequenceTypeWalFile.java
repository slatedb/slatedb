package io.slatedb.uniffi;


import java.util.List;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public enum FfiConverterSequenceTypeWalFile implements FfiConverterRustBuffer<List<WalFile>> {
  INSTANCE;

  @Override
  public List<WalFile> read(ByteBuffer buf) {
    int len = buf.getInt();
    return IntStream.range(0, len).mapToObj(_i -> FfiConverterTypeWalFile.INSTANCE.read(buf)).toList();
  }

  @Override
  public long allocationSize(List<WalFile> value) {
    long sizeForLength = 4L;
    long sizeForItems = value.stream().mapToLong(inner -> FfiConverterTypeWalFile.INSTANCE.allocationSize(inner)).sum();
    return sizeForLength + sizeForItems;
  }

  @Override
  public void write(List<WalFile> value, ByteBuffer buf) {
    buf.putInt(value.size());
    value.forEach(inner -> FfiConverterTypeWalFile.INSTANCE.write(inner, buf));
  }
}



