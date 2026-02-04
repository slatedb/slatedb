package io.slatedb;

/// Key/value pair returned by scan iterators.
public record KeyValue(byte[] key, byte[] value) {}
