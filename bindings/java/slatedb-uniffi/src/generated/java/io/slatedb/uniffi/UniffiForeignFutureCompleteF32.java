package io.slatedb.uniffi;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteF32 extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructF32.UniffiByValue result);
}
