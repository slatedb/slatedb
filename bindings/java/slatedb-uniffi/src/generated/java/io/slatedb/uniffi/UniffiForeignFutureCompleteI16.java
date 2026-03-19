package io.slatedb.uniffi;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteI16 extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructI16.UniffiByValue result);
}
