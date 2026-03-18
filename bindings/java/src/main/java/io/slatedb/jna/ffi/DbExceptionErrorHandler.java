package io.slatedb.jna.ffi;

public class DbExceptionErrorHandler implements UniffiRustCallStatusErrorHandler<DbException> {
  @Override
  public DbException lift(RustBuffer.ByValue errorBuf) {
    return FfiConverterTypeDbError.INSTANCE.lift(errorBuf);
  }
}
