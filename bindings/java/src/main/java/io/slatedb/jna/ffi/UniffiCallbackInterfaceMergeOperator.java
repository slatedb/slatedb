package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

// Put the implementation in an object so we don't pollute the top-level namespace
public class UniffiCallbackInterfaceMergeOperator {
  public static final UniffiCallbackInterfaceMergeOperator INSTANCE =
      new UniffiCallbackInterfaceMergeOperator();
  UniffiVTableCallbackInterfaceMergeOperator.UniffiByValue vtable;

  UniffiCallbackInterfaceMergeOperator() {
    vtable =
        new UniffiVTableCallbackInterfaceMergeOperator.UniffiByValue(
            merge.INSTANCE, UniffiFree.INSTANCE);
  }

  // Registers the foreign callback with the Rust side.
  // This method is generated for each callback interface.
  void register(UniffiLib lib) {
    lib.uniffi_slatedb_ffi_fn_init_callback_vtable_mergeoperator(vtable);
  }

  public static class merge implements UniffiCallbackInterfaceMergeOperatorMethod0 {
    public static final merge INSTANCE = new merge();

    private merge() {}

    @Override
    public void callback(
        long uniffiHandle,
        RustBuffer.ByValue key,
        RustBuffer.ByValue existingValue,
        RustBuffer.ByValue operand,
        RustBuffer uniffiOutReturn,
        UniffiRustCallStatus uniffiCallStatus) {
      var uniffiObj = FfiConverterTypeMergeOperator.INSTANCE.handleMap.get(uniffiHandle);
      Callable<byte[]> makeCall =
          () -> {
            return uniffiObj.merge(
                FfiConverterByteArray.INSTANCE.lift(key),
                FfiConverterOptionalByteArray.INSTANCE.lift(existingValue),
                FfiConverterByteArray.INSTANCE.lift(operand));
          };
      Consumer<byte[]> writeReturn =
          (byte[] value) -> {
            uniffiOutReturn.setValue(FfiConverterByteArray.INSTANCE.lower(value));
          };
      UniffiHelpers.uniffiTraitInterfaceCallWithError(
          uniffiCallStatus,
          makeCall,
          writeReturn,
          (MergeOperatorCallbackException e) -> {
            return FfiConverterTypeMergeOperatorCallbackError.INSTANCE.lower(e);
          },
          MergeOperatorCallbackException.class);
    }
  }

  public static class UniffiFree implements UniffiCallbackInterfaceFree {
    public static final UniffiFree INSTANCE = new UniffiFree();

    private UniffiFree() {}

    @Override
    public void callback(long handle) {
      FfiConverterTypeMergeOperator.INSTANCE.handleMap.remove(handle);
    }
  }
}
