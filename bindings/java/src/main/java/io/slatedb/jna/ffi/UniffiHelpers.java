package io.slatedb.jna.ffi;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

// Helpers for calling Rust
// In practice we usually need to be synchronized to call this safely, so it doesn't
// synchronize itself
public final class UniffiHelpers {
  // Call a rust function that returns a Result<>.  Pass in the Error class companion that
  // corresponds to the Err
  static <U, E extends Exception> U uniffiRustCallWithError(
      UniffiRustCallStatusErrorHandler<E> errorHandler, Function<UniffiRustCallStatus, U> callback)
      throws E {
    UniffiRustCallStatus status = new UniffiRustCallStatus();
    U returnValue = callback.apply(status);
    uniffiCheckCallStatus(errorHandler, status);
    return returnValue;
  }

  // Overload to call a rust function that returns a Result<()>, because void is outside Java's type
  // system.  Pass in the Error class companion that corresponds to the Err
  static <E extends Exception> void uniffiRustCallWithError(
      UniffiRustCallStatusErrorHandler<E> errorHandler, Consumer<UniffiRustCallStatus> callback)
      throws E {
    UniffiRustCallStatus status = new UniffiRustCallStatus();
    callback.accept(status);
    uniffiCheckCallStatus(errorHandler, status);
  }

  // Check UniffiRustCallStatus and throw an error if the call wasn't successful
  static <E extends Exception> void uniffiCheckCallStatus(
      UniffiRustCallStatusErrorHandler<E> errorHandler, UniffiRustCallStatus status) throws E {
    if (status.isSuccess()) {
      return;
    } else if (status.isError()) {
      throw errorHandler.lift(status.error_buf);
    } else if (status.isPanic()) {
      // when the rust code sees a panic, it tries to construct a rustbuffer
      // with the message.  but if that code panics, then it just sends back
      // an empty buffer.
      if (status.error_buf.len > 0) {
        throw new InternalException(FfiConverterString.INSTANCE.lift(status.error_buf));
      } else {
        throw new InternalException("Rust panic");
      }
    } else {
      throw new InternalException("Unknown rust call status: " + status.code);
    }
  }

  // Call a rust function that returns a plain value
  static <U> U uniffiRustCall(Function<UniffiRustCallStatus, U> callback) {
    return uniffiRustCallWithError(new UniffiNullRustCallStatusErrorHandler(), callback);
  }

  // Call a rust function that returns nothing
  static void uniffiRustCall(Consumer<UniffiRustCallStatus> callback) {
    uniffiRustCallWithError(new UniffiNullRustCallStatusErrorHandler(), callback);
  }

  static <T> void uniffiTraitInterfaceCall(
      UniffiRustCallStatus callStatus, Supplier<T> makeCall, Consumer<T> writeReturn) {
    try {
      writeReturn.accept(makeCall.get());
    } catch (Exception e) {
      callStatus.setCode(UniffiRustCallStatus.UNIFFI_CALL_UNEXPECTED_ERROR);
      callStatus.setErrorBuf(FfiConverterString.INSTANCE.lower(e.toString()));
    }
  }

  static <T, E extends Throwable> void uniffiTraitInterfaceCallWithError(
      UniffiRustCallStatus callStatus,
      Callable<T> makeCall,
      Consumer<T> writeReturn,
      Function<E, RustBuffer.ByValue> lowerError,
      Class<E> errorClazz) {
    try {
      writeReturn.accept(makeCall.call());
    } catch (Exception e) {
      if (errorClazz.isAssignableFrom(e.getClass())) {
        @SuppressWarnings("unchecked")
        E castedE = (E) e;
        callStatus.setCode(UniffiRustCallStatus.UNIFFI_CALL_ERROR);
        callStatus.setErrorBuf(lowerError.apply(castedE));
      } else {
        callStatus.setCode(UniffiRustCallStatus.UNIFFI_CALL_UNEXPECTED_ERROR);
        callStatus.setErrorBuf(FfiConverterString.INSTANCE.lower(e.toString()));
      }
    }
  }
}
