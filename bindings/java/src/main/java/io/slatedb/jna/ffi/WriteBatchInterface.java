package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

public interface WriteBatchInterface {

  public void delete(byte[] key) throws DbException;

  public void merge(byte[] key, byte[] operand) throws DbException;

  public void mergeWithOptions(byte[] key, byte[] operand, MergeOptions options) throws DbException;

  public void put(byte[] key, byte[] value) throws DbException;

  public void putWithOptions(byte[] key, byte[] value, PutOptions options) throws DbException;
}
