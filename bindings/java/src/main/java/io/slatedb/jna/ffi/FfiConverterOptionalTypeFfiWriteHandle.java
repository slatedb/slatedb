package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;

// public class TestForOptionals {}
public enum FfiConverterOptionalTypeFfiWriteHandle
    implements FfiConverterRustBuffer<FfiWriteHandle> {
  INSTANCE;

  @Override
  public FfiWriteHandle read(ByteBuffer buf) {
    if (buf.get() == (byte) 0) {
      return null;
    }
    return FfiConverterTypeFfiWriteHandle.INSTANCE.read(buf);
  }

  @Override
  public long allocationSize(FfiWriteHandle value) {
    if (value == null) {
      return 1L;
    } else {
      return 1L + FfiConverterTypeFfiWriteHandle.INSTANCE.allocationSize(value);
    }
  }

  @Override
  public void write(FfiWriteHandle value, ByteBuffer buf) {
    if (value == null) {
      buf.put((byte) 0);
    } else {
      buf.put((byte) 1);
      FfiConverterTypeFfiWriteHandle.INSTANCE.write(value, buf);
    }
  }
}

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
