package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteU8 extends Callback {
  public void callback(long callbackData, UniffiForeignFutureStructU8.UniffiByValue result);
}
