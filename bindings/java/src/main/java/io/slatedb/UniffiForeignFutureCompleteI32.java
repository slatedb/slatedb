package io.slatedb;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteI32 extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructI32.UniffiByValue result);
}
