package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteU16 extends Callback {
  public void callback(long callbackData, UniffiForeignFutureStructU16.UniffiByValue result);
}
