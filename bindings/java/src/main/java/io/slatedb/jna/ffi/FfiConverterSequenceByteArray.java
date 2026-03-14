package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

public enum FfiConverterSequenceByteArray implements FfiConverterRustBuffer<List<byte[]>> {
  INSTANCE;

  @Override
  public List<byte[]> read(ByteBuffer buf) {
    int len = buf.getInt();
    return IntStream.range(0, len)
        .mapToObj(_i -> FfiConverterByteArray.INSTANCE.read(buf))
        .toList();
  }

  @Override
  public long allocationSize(List<byte[]> value) {
    long sizeForLength = 4L;
    long sizeForItems =
        value.stream()
            .mapToLong(inner -> FfiConverterByteArray.INSTANCE.allocationSize(inner))
            .sum();
    return sizeForLength + sizeForItems;
  }

  @Override
  public void write(List<byte[]> value, ByteBuffer buf) {
    buf.putInt(value.size());
    value.forEach(inner -> FfiConverterByteArray.INSTANCE.write(inner, buf));
  }
}

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
