package io.slatedb;


import java.nio.ByteBuffer;

public enum FfiConverterTypeDbWriteOperation implements FfiConverterRustBuffer<DbWriteOperation> {
    INSTANCE;

    @Override
    public DbWriteOperation read(ByteBuffer buf) {
      return switch (buf.getInt()) {
        case 1 -> new DbWriteOperation.Put(FfiConverterByteArray.INSTANCE.read(buf),
          FfiConverterByteArray.INSTANCE.read(buf),
          FfiConverterTypeDbPutOptions.INSTANCE.read(buf)
          );
        case 2 -> new DbWriteOperation.Merge(FfiConverterByteArray.INSTANCE.read(buf),
          FfiConverterByteArray.INSTANCE.read(buf),
          FfiConverterTypeDbMergeOptions.INSTANCE.read(buf)
          );
        case 3 -> new DbWriteOperation.Delete(FfiConverterByteArray.INSTANCE.read(buf)
          );
        default ->
          throw new RuntimeException("invalid enum value, something is very wrong!");
      };
    }

    @Override
    public long allocationSize(DbWriteOperation value) {
        return switch (value) {
          case DbWriteOperation.Put(var key, var valueBytes, var options) ->
            (4L
            + FfiConverterByteArray.INSTANCE.allocationSize(key)
            + FfiConverterByteArray.INSTANCE.allocationSize(valueBytes)
            + FfiConverterTypeDbPutOptions.INSTANCE.allocationSize(options));
          case DbWriteOperation.Merge(var key, var operand, var options) ->
            (4L
            + FfiConverterByteArray.INSTANCE.allocationSize(key)
            + FfiConverterByteArray.INSTANCE.allocationSize(operand)
            + FfiConverterTypeDbMergeOptions.INSTANCE.allocationSize(options));
          case DbWriteOperation.Delete(var key) ->
            (4L
            + FfiConverterByteArray.INSTANCE.allocationSize(key));
        };
    }

    @Override
    public void write(DbWriteOperation value, ByteBuffer buf) {
      switch (value) {
        case DbWriteOperation.Put(var key, var valueBytes, var options) -> {
          buf.putInt(1);
          FfiConverterByteArray.INSTANCE.write(key, buf);
          FfiConverterByteArray.INSTANCE.write(valueBytes, buf);
          FfiConverterTypeDbPutOptions.INSTANCE.write(options, buf);
        }
        case DbWriteOperation.Merge(var key, var operand, var options) -> {
          buf.putInt(2);
          FfiConverterByteArray.INSTANCE.write(key, buf);
          FfiConverterByteArray.INSTANCE.write(operand, buf);
          FfiConverterTypeDbMergeOptions.INSTANCE.write(options, buf);
        }
        case DbWriteOperation.Delete(var key) -> {
          buf.putInt(3);
          FfiConverterByteArray.INSTANCE.write(key, buf);
        }
      };
    }
}




