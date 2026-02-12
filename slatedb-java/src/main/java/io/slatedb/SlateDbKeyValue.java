package io.slatedb;

import java.nio.ByteBuffer;

/// Key/value pair returned by scan iterators.
public record SlateDbKeyValue(ByteBuffer key, ByteBuffer value) {}
