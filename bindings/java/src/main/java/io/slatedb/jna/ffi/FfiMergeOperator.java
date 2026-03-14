package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

public interface FfiMergeOperator {

  public byte[] merge(byte[] key, byte[] existingValue, byte[] operand)
      throws FfiMergeOperatorCallbackException;
}
