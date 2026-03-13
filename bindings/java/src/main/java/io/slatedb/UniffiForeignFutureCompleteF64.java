package io.slatedb;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteF64 extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructF64.UniffiByValue result);
}
