package io.slatedb;

/// Key/value pair returned by scan iterators.
public record SlateDbKeyValue(byte[] key, byte[] value) {}
