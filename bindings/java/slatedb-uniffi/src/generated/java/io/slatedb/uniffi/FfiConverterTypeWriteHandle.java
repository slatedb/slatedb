package io.slatedb.uniffi;


import java.nio.ByteBuffer;

public enum FfiConverterTypeWriteHandle implements FfiConverterRustBuffer<WriteHandle> {
  INSTANCE;

  @Override
  public WriteHandle read(ByteBuffer buf) {
    return new WriteHandle(
      FfiConverterLong.INSTANCE.read(buf),
      FfiConverterLong.INSTANCE.read(buf)
    );
  }

  @Override
  public long allocationSize(WriteHandle value) {
      return (
            FfiConverterLong.INSTANCE.allocationSize(value.seqnum()) +
            FfiConverterLong.INSTANCE.allocationSize(value.createTs())
      );
  }

  @Override
  public void write(WriteHandle value, ByteBuffer buf) {
      FfiConverterLong.INSTANCE.write(value.seqnum(), buf);
      FfiConverterLong.INSTANCE.write(value.createTs(), buf);
  }
}



