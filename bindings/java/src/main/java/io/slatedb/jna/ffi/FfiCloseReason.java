package io.slatedb.jna.ffi;


public enum FfiCloseReason {
  NONE,
  CLEAN,
  FENCED,
  PANIC,
  UNKNOWN;
}
