package io.slatedb.jna.ffi;

import com.sun.jna.*;
import com.sun.jna.ptr.*;

public interface LogCallback {

  public void log(LogRecord record);
}
