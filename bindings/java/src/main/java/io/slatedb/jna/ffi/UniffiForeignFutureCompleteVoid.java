package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiForeignFutureCompleteVoid extends Callback {
  public void callback(long callbackData, UniffiForeignFutureStructVoid.UniffiByValue result);
}
