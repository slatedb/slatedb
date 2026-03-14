package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiCallbackInterfaceFfiLogCallbackMethod0 extends Callback {
  public void callback(
      long uniffiHandle,
      RustBuffer.ByValue record,
      Pointer uniffiOutReturn,
      UniffiRustCallStatus uniffiCallStatus);
}
