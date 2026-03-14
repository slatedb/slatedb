package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

public enum FfiConverterSequenceTypeFfiWalFile implements FfiConverterRustBuffer<List<FfiWalFile>> {
  INSTANCE;

  @Override
  public List<FfiWalFile> read(ByteBuffer buf) {
    int len = buf.getInt();
    return IntStream.range(0, len)
        .mapToObj(_i -> FfiConverterTypeFfiWalFile.INSTANCE.read(buf))
        .toList();
  }

  @Override
  public long allocationSize(List<FfiWalFile> value) {
    long sizeForLength = 4L;
    long sizeForItems =
        value.stream()
            .mapToLong(inner -> FfiConverterTypeFfiWalFile.INSTANCE.allocationSize(inner))
            .sum();
    return sizeForLength + sizeForItems;
  }

  @Override
  public void write(List<FfiWalFile> value, ByteBuffer buf) {
    buf.putInt(value.size());
    value.forEach(inner -> FfiConverterTypeFfiWalFile.INSTANCE.write(inner, buf));
  }
}
