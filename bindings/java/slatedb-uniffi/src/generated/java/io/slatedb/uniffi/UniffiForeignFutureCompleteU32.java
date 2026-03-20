package io.slatedb.uniffi;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteU32 extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructU32.UniffiByValue result);
}
