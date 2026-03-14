package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;

// This is a helper for safely passing byte references into the rust code.
// It's not actually used at the moment, because there aren't many things that you
// can take a direct pointer to in the JVM, and if we're going to copy something
// then we might as well copy it into a `RustBuffer`. But it's here for API
// completeness.
@Structure.FieldOrder({"len", "data"})
public class ForeignBytes extends Structure {
  public int len;
  public Pointer data;

  public static class ByValue extends ForeignBytes implements Structure.ByValue {}
}
