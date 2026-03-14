package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteI8 extends Callback {
  public void callback(long callbackData, UniffiForeignFutureStructI8.UniffiByValue result);
}
