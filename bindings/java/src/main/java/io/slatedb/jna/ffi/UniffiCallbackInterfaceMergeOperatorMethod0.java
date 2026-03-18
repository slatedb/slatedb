package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

interface UniffiCallbackInterfaceMergeOperatorMethod0 extends Callback {
  public void callback(
      long uniffiHandle,
      RustBuffer.ByValue key,
      RustBuffer.ByValue existingValue,
      RustBuffer.ByValue operand,
      RustBuffer uniffiOutReturn,
      UniffiRustCallStatus uniffiCallStatus);
}
