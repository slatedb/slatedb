package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiLogRecord {
  private FfiLogLevel level;
  private String target;
  private String message;
  private String modulePath;
  private String file;
  private Integer line;

  public FfiLogRecord(
      FfiLogLevel level,
      String target,
      String message,
      String modulePath,
      String file,
      Integer line) {

    this.level = level;

    this.target = target;

    this.message = message;

    this.modulePath = modulePath;

    this.file = file;

    this.line = line;
  }

  public FfiLogLevel level() {
    return this.level;
  }

  public String target() {
    return this.target;
  }

  public String message() {
    return this.message;
  }

  public String modulePath() {
    return this.modulePath;
  }

  public String file() {
    return this.file;
  }

  public Integer line() {
    return this.line;
  }

  public void setLevel(FfiLogLevel level) {
    this.level = level;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void setModulePath(String modulePath) {
    this.modulePath = modulePath;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public void setLine(Integer line) {
    this.line = line;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiLogRecord) {
      FfiLogRecord t = (FfiLogRecord) other;
      return (Objects.equals(level, t.level)
          && Objects.equals(target, t.target)
          && Objects.equals(message, t.message)
          && Objects.equals(modulePath, t.modulePath)
          && Objects.equals(file, t.file)
          && Objects.equals(line, t.line));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, target, message, modulePath, file, line);
  }
}
