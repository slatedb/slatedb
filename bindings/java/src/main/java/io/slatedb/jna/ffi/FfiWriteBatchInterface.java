package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

public interface FfiWriteBatchInterface {

  public void delete(byte[] key) throws FfiException;

  public void merge(byte[] key, byte[] operand) throws FfiException;

  public void mergeWithOptions(byte[] key, byte[] operand, FfiMergeOptions options)
      throws FfiException;

  public void put(byte[] key, byte[] value) throws FfiException;

  public void putWithOptions(byte[] key, byte[] value, FfiPutOptions options) throws FfiException;
}
