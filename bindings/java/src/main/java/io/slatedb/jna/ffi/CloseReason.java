package io.slatedb.jna.ffi;


public enum CloseReason {
  NONE,
  CLEAN,
  FENCED,
  PANIC,
  UNKNOWN;
}
