package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiError implements FfiConverterRustBuffer<FfiException> {
  INSTANCE;

  @Override
  public FfiException read(ByteBuffer buf) {

    return switch (buf.getInt()) {
      case 1 -> new FfiException.Transaction(FfiConverterString.INSTANCE.read(buf));
      case 2 ->
          new FfiException.Closed(
              FfiConverterTypeFfiCloseReason.INSTANCE.read(buf),
              FfiConverterString.INSTANCE.read(buf));
      case 3 -> new FfiException.Unavailable(FfiConverterString.INSTANCE.read(buf));
      case 4 -> new FfiException.Invalid(FfiConverterString.INSTANCE.read(buf));
      case 5 -> new FfiException.Data(FfiConverterString.INSTANCE.read(buf));
      case 6 -> new FfiException.Internal(FfiConverterString.INSTANCE.read(buf));
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    };
  }

  @Override
  public long allocationSize(FfiException value) {
    return switch (value) {
      case FfiException.Transaction x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case FfiException.Closed x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L
              + FfiConverterTypeFfiCloseReason.INSTANCE.allocationSize(x.reason)
              + FfiConverterString.INSTANCE.allocationSize(x.message));
      case FfiException.Unavailable x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case FfiException.Invalid x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case FfiException.Data x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      case FfiException.Internal x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    };
  }

  @Override
  public void write(FfiException value, ByteBuffer buf) {
    switch (value) {
      case FfiException.Transaction x -> {
        buf.putInt(1);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case FfiException.Closed x -> {
        buf.putInt(2);
        FfiConverterTypeFfiCloseReason.INSTANCE.write(x.reason, buf);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case FfiException.Unavailable x -> {
        buf.putInt(3);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case FfiException.Invalid x -> {
        buf.putInt(4);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case FfiException.Data x -> {
        buf.putInt(5);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      case FfiException.Internal x -> {
        buf.putInt(6);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    }
    ;
  }
}
