package io.slatedb;


import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureFree extends Callback {
    public void callback(long handle);
}
