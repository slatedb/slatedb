package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiRustFutureContinuationCallback extends Callback {
  public void callback(long data, byte pollResult);
}
