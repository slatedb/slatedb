package io.slatedb.jna.ffi;


public sealed interface FfiTtl {

  record Default() implements FfiTtl {}

  record NoExpiry() implements FfiTtl {}

  record ExpireAfterTicks(Long v1) implements FfiTtl {}
}
