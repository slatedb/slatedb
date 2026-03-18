package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeWriteHandle implements FfiConverterRustBuffer<WriteHandle> {
  INSTANCE;

  @Override
  public WriteHandle read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeWriteHandle.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(WriteHandle value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeWriteHandle.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(WriteHandle value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeWriteHandle.INSTANCE.write(value, buf);
    }
  }
}

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
