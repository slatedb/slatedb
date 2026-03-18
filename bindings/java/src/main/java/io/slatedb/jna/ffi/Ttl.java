package io.slatedb.jna.ffi;


public sealed interface Ttl {

  record Default() implements Ttl {}

  record NoExpiry() implements Ttl {}

  record ExpireAfterTicks(Long v1) implements Ttl {}
}
