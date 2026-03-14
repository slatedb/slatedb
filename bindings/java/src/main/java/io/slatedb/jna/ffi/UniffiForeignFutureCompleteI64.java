package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteI64 extends Callback {
  public void callback(long callbackData, UniffiForeignFutureStructI64.UniffiByValue result);
}
