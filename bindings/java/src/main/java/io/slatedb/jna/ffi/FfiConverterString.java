package io.slatedb.jna.ffi;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public enum FfiConverterString implements FfiConverter<String, RustBuffer.ByValue> {
  INSTANCE;

  // Note: we don't inherit from FfiConverterRustBuffer, because we use a
  // special encoding when lowering/lifting.  We can use `RustBuffer.len` to
  // store our length and avoid writing it out to the buffer.
  @Override
  public String lift(RustBuffer.ByValue value) {
    try {
      byte[] byteArr = new byte[(int) value.len];
      value.asByteBuffer().get(byteArr);
      return new String(byteArr, StandardCharsets.UTF_8);
    } finally {
      RustBuffer.free(value);
    }
  }

  @Override
  public String read(ByteBuffer buf) {
    int len = buf.getInt();
    byte[] byteArr = new byte[len];
    buf.get(byteArr);
    return new String(byteArr, StandardCharsets.UTF_8);
  }

  private ByteBuffer toUtf8(String value) {
    // Make sure we don't have invalid UTF-16, check for lone surrogates.
    CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
    encoder.onMalformedInput(CodingErrorAction.REPORT);
    try {
      return encoder.encode(CharBuffer.wrap(value));
    } catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RustBuffer.ByValue lower(String value) {
    ByteBuffer byteBuf = toUtf8(value);
    // Ideally we'd pass these bytes to `ffi_bytebuffer_from_bytes`, but doing so would require us
    // to copy them into a JNA `Memory`. So we might as well directly copy them into a `RustBuffer`.
    RustBuffer.ByValue rbuf = RustBuffer.alloc((long) byteBuf.limit());
    rbuf.asByteBuffer().put(byteBuf);
    return rbuf;
  }

  // We aren't sure exactly how many bytes our string will be once it's UTF-8
  // encoded.  Allocate 3 bytes per UTF-16 code unit which will always be
  // enough.
  @Override
  public long allocationSize(String value) {
    long sizeForLength = 4L;
    long sizeForString = (long) value.length() * 3L;
    return sizeForLength + sizeForString;
  }

  @Override
  public void write(String value, ByteBuffer buf) {
    ByteBuffer byteBuf = toUtf8(value);
    buf.putInt(byteBuf.limit());
    buf.put(byteBuf);
  }
}
