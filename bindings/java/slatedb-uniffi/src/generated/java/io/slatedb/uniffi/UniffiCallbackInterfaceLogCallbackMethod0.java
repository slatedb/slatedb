package io.slatedb.uniffi;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiCallbackInterfaceLogCallbackMethod0 extends Callback {
    public void callback(long uniffiHandle,RustBuffer.ByValue record,Pointer uniffiOutReturn,
        UniffiRustCallStatus uniffiCallStatus);
}
