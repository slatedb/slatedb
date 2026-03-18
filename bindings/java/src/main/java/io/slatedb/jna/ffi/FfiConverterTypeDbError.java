package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeDbError implements FfiConverterRustBuffer<DbException> {
  INSTANCE;

  @Override
  public DbException read(ByteBuffer buf) {

    return switch (buf.getInt()) {
      case 1 -> new DbException.Transaction(FfiConverterString.INSTANCE.read(buf));
      case 2 ->
          new DbException.Closed(
              FfiConverterTypeCloseReason.INSTANCE.read(buf),
              FfiConverterString.INSTANCE.read(buf));
      case 3 -> new DbException.Unavailable(FfiConverterString.INSTANCE.read(buf));
      case 4 -> new DbException.Invalid(FfiConverterString.INSTANCE.read(buf));
      case 5 -> new DbException.Data(FfiConverterString.INSTANCE.read(buf));
      case 6 -> new DbException.Internal(FfiConverterString.INSTANCE.read(buf));
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    };
  }

  @Override
  public long allocationSize(DbException value) {
    return switch (value) {
      case DbException.Transaction x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case DbException.Closed x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L
              + FfiConverterTypeCloseReason.INSTANCE.allocationSize(x.reason)
              + FfiConverterString.INSTANCE.allocationSize(x.message));
      case DbException.Unavailable x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case DbException.Invalid x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case DbException.Data x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case DbException.Internal x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    };
  }

  @Override
  public void write(DbException value, ByteBuffer buf) {
    switch (value) {
      case DbException.Transaction x -> {
        buf.putInt(1);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case DbException.Closed x -> {
        buf.putInt(2);
        FfiConverterTypeCloseReason.INSTANCE.write(x.reason, buf);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case DbException.Unavailable x -> {
        buf.putInt(3);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case DbException.Invalid x -> {
        buf.putInt(4);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case DbException.Data x -> {
        buf.putInt(5);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case DbException.Internal x -> {
        buf.putInt(6);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    }
    ;
  }
}
