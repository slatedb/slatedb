package io.slatedb;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteU64 extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructU64.UniffiByValue result);
}
