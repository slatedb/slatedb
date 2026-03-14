package io.slatedb.jna.ffi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public final class UniffiAsyncHelpers {
  // Async return type handlers
  static final byte UNIFFI_RUST_FUTURE_POLL_READY = (byte) 0;
  static final byte UNIFFI_RUST_FUTURE_POLL_MAYBE_READY = (byte) 1;
  static final UniffiHandleMap<CompletableFuture<Byte>> uniffiContinuationHandleMap =
      new UniffiHandleMap<>();
  static final UniffiHandleMap<CancelableForeignFuture> uniffiForeignFutureHandleMap =
      new UniffiHandleMap<>();

  // FFI type for Rust future continuations
  enum UniffiRustFutureContinuationCallbackImpl implements UniffiRustFutureContinuationCallback {
    INSTANCE;

    @Override
    public void callback(long data, byte pollResult) {
      uniffiContinuationHandleMap.remove(data).complete(pollResult);
    }
  }

  @FunctionalInterface
  interface PollingFunction {
    void apply(
        long rustFuture, UniffiRustFutureContinuationCallback callback, long continuationHandle);
  }

  static class UniffiFreeingFuture<T> extends CompletableFuture<T> {
    private Consumer<Long> freeFunc;
    private long rustFuture;

    public UniffiFreeingFuture(long rustFuture, Consumer<Long> freeFunc) {
      this.freeFunc = freeFunc;
      this.rustFuture = rustFuture;
    }

    @Override
    public boolean cancel(boolean ignored) {
      boolean cancelled = super.cancel(ignored);
      if (cancelled) {
        freeFunc.accept(rustFuture);
      }
      return cancelled;
    }
  }

  // helper so both the Java completable future and the job that handles it finishing and reports to
  // Rust can be
  // retrieved (and potentially cancelled) by handle. This allows our FreeImpl to be a parameterless
  // singleton,
  // preventing #19, which was caused by our FreeImpls being GCd before Rust called back into them.
  static class CancelableForeignFuture {
    private CompletableFuture<?> childFuture;
    private CompletableFuture<Void> childFutureHandler;

    CancelableForeignFuture(
        CompletableFuture<?> childFuture, CompletableFuture<Void> childFutureHandler) {
      this.childFuture = childFuture;
      this.childFutureHandler = childFutureHandler;
    }

    public void cancel() {
      var successfullyCancelled = this.childFutureHandler.cancel(true);
      if (successfullyCancelled) {
        childFuture.cancel(true);
      }
    }
  }

  static <T, F, E extends Exception> CompletableFuture<T> uniffiRustCallAsync(
      long rustFuture,
      PollingFunction pollFunc,
      BiFunction<Long, UniffiRustCallStatus, F> completeFunc,
      Consumer<Long> freeFunc,
      Function<F, T> liftFunc,
      UniffiRustCallStatusErrorHandler<E> errorHandler) {
    CompletableFuture<T> future = new UniffiFreeingFuture<>(rustFuture, freeFunc);

    CompletableFuture.runAsync(
        () -> {
          try {
            byte pollResult;
            do {
              pollResult = poll(rustFuture, pollFunc);
            } while (pollResult != UNIFFI_RUST_FUTURE_POLL_READY);

            if (!future.isCancelled()) {
              F result =
                  UniffiHelpers.uniffiRustCallWithError(
                      errorHandler,
                      status -> {
                        return completeFunc.apply(rustFuture, status);
                      });
              T liftedResult = liftFunc.apply(result);
              future.complete(liftedResult);
            }
          } catch (Exception e) {
            future.completeExceptionally(e);
          } finally {
            if (!future.isCancelled()) {
              freeFunc.accept(rustFuture);
            }
          }
        });

    return future;
  }

  // overload specifically for Void cases, which aren't within the Object type.
  // This is only necessary because of Java's lack of proper Any/Unit
  static <E extends Exception> CompletableFuture<Void> uniffiRustCallAsync(
      long rustFuture,
      PollingFunction pollFunc,
      BiConsumer<Long, UniffiRustCallStatus> completeFunc,
      Consumer<Long> freeFunc,
      Runnable liftFunc,
      UniffiRustCallStatusErrorHandler<E> errorHandler) {
    CompletableFuture<Void> future = new UniffiFreeingFuture<>(rustFuture, freeFunc);

    CompletableFuture.runAsync(
        () -> {
          try {
            byte pollResult;
            do {
              pollResult = poll(rustFuture, pollFunc);
            } while (pollResult != UNIFFI_RUST_FUTURE_POLL_READY);

            // even though the outer `future` has been cancelled, this inner `runAsync` is
            // unsupervised
            // and keeps running. When it calls `completeFunc` after being cancelled, it's status is
            // `SUCCESS`
            // (assuming the Rust part succeeded), and the function being called can lead to a core
            // dump.
            // Guarding with `isCancelled` here makes everything work, but feels like a cludge.
            if (!future.isCancelled()) {
              UniffiHelpers.uniffiRustCallWithError(
                  errorHandler,
                  status -> {
                    completeFunc.accept(rustFuture, status);
                  });
              future.complete(null);
            }
          } catch (Throwable e) {
            future.completeExceptionally(e);
          } finally {
            if (!future.isCancelled()) {
              freeFunc.accept(rustFuture);
            }
          }
        });

    return future;
  }

  private static byte poll(long rustFuture, PollingFunction pollFunc)
      throws InterruptedException, ExecutionException {
    CompletableFuture<Byte> pollFuture = new CompletableFuture<>();
    var handle = uniffiContinuationHandleMap.insert(pollFuture);
    pollFunc.apply(rustFuture, UniffiRustFutureContinuationCallbackImpl.INSTANCE, handle);
    do {} while (!pollFuture.isDone()); // removing this makes futures not cancel (sometimes)
    return pollFuture.get();
  }
}

// Public interface members begin here.
