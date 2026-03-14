package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

public enum FfiConverterTypeFfiMergeOperatorCallbackError
    implements FfiConverterRustBuffer<FfiMergeOperatorCallbackException> {
  INSTANCE;

  @Override
  public FfiMergeOperatorCallbackException read(ByteBuffer buf) {

    return switch (buf.getInt()) {
      case 1 -> new FfiMergeOperatorCallbackException.Failed(FfiConverterString.INSTANCE.read(buf));
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    };
  }

  @Override
  public long allocationSize(FfiMergeOperatorCallbackException value) {
    return switch (value) {
      case FfiMergeOperatorCallbackException.Failed x ->
          (
          // Add the size for the Int that specifies the variant plus the size needed for all fields
          4L + FfiConverterString.INSTANCE.allocationSize(x.message));
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    };
  }

  @Override
  public void write(FfiMergeOperatorCallbackException value, ByteBuffer buf) {
    switch (value) {
      case FfiMergeOperatorCallbackException.Failed x -> {
        buf.putInt(1);
        FfiConverterString.INSTANCE.write(x.message, buf);
      }
      default -> throw new RuntimeException("invalid error enum value, something is very wrong!!");
    }
    ;
  }
}
