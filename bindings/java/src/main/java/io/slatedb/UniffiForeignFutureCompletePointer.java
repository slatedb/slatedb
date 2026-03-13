package io.slatedb;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompletePointer extends Callback {
    public void callback(long callbackData,UniffiForeignFutureStructPointer.UniffiByValue result);
}
