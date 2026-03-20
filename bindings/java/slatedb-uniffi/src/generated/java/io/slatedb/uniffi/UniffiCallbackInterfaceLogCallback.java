package io.slatedb.uniffi;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.List;
import com.sun.jna.*;
import com.sun.jna.ptr.*;

// Put the implementation in an object so we don't pollute the top-level namespace
public class UniffiCallbackInterfaceLogCallback {
    public static final UniffiCallbackInterfaceLogCallback INSTANCE = new UniffiCallbackInterfaceLogCallback();
    UniffiVTableCallbackInterfaceLogCallback.UniffiByValue vtable;
    
    UniffiCallbackInterfaceLogCallback() {
        vtable = new UniffiVTableCallbackInterfaceLogCallback.UniffiByValue(
            log.INSTANCE,
            UniffiFree.INSTANCE
        );
    }
    
    // Registers the foreign callback with the Rust side.
    // This method is generated for each callback interface.
    void register(UniffiLib lib) {
        lib.uniffi_slatedb_uniffi_fn_init_callback_vtable_logcallback(vtable);
    }
    
    public static class log implements UniffiCallbackInterfaceLogCallbackMethod0 {
        public static final log INSTANCE = new log();
        private log() {}

        @Override
        public void callback(long uniffiHandle,RustBuffer.ByValue record,Pointer uniffiOutReturn,UniffiRustCallStatus uniffiCallStatus) {
            var uniffiObj = FfiConverterTypeLogCallback.INSTANCE.handleMap.get(uniffiHandle);
            Supplier<Void> makeCall = () -> {
                uniffiObj.log(
                    FfiConverterTypeLogRecord.INSTANCE.lift(record)
                );
                return null;
            };
            Consumer<Void> writeReturn = (nothing) -> {};
            UniffiHelpers.uniffiTraitInterfaceCall(uniffiCallStatus, makeCall, writeReturn);
        }
    }

    public static class UniffiFree implements UniffiCallbackInterfaceFree {
        public static final UniffiFree INSTANCE = new UniffiFree();

        private UniffiFree() {}

        @Override
        public void callback(long handle) {
            FfiConverterTypeLogCallback.INSTANCE.handleMap.remove(handle);
        }
    }
}

