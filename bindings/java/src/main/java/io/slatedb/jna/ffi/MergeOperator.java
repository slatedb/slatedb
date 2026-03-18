package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

public interface MergeOperator {

  public byte[] merge(byte[] key, byte[] existingValue, byte[] operand)
      throws MergeOperatorCallbackException;
}
