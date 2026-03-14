package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiCallbackInterfaceFree extends Callback {
  public void callback(long handle);
}
