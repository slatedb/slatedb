package io.slatedb.jna.ffi;

public class FfiException extends Exception {
  private FfiException(String message) {
    super(message);
  }

  public static class Transaction extends FfiException {

    String message;

    public Transaction(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Closed extends FfiException {

    FfiCloseReason reason;

    String message;

    public Closed(FfiCloseReason reason, String message) {
      super(
          new StringBuilder()
              .append("reason=")
              .append(reason)
              .append(", ")
              .append("message=")
              .append(message)
              .toString());
      this.reason = reason;
      this.message = message;
    }

    public FfiCloseReason reason() {
      return this.reason;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Unavailable extends FfiException {

    String message;

    public Unavailable(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Invalid extends FfiException {

    String message;

    public Invalid(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Data extends FfiException {

    String message;

    public Data(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Internal extends FfiException {

    String message;

    public Internal(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }
}
