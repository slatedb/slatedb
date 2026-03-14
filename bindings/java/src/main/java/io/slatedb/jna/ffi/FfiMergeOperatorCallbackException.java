package io.slatedb.jna.ffi;

public class FfiMergeOperatorCallbackException extends Exception {
  private FfiMergeOperatorCallbackException(String message) {
    super(message);
  }

  public static class Failed extends FfiMergeOperatorCallbackException {

    String message;

    public Failed(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }
}
