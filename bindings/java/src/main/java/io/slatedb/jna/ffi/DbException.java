package io.slatedb.jna.ffi;

public class DbException extends Exception {
  private DbException(String message) {
    super(message);
  }

  public static class Transaction extends DbException {

    String message;

    public Transaction(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Closed extends DbException {

    CloseReason reason;

    String message;

    public Closed(CloseReason reason, String message) {
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

    public CloseReason reason() {
      return this.reason;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Unavailable extends DbException {

    String message;

    public Unavailable(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Invalid extends DbException {

    String message;

    public Invalid(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Data extends DbException {

    String message;

    public Data(String message) {
      super(new StringBuilder().append("message=").append(message).toString());

      this.message = message;
    }

    public String message() {
      return this.message;
    }
  }

  public static class Internal extends DbException {

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
