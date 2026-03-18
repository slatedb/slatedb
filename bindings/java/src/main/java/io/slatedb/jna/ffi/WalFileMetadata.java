package io.slatedb.jna.ffi;

import java.util.Objects;

public class WalFileMetadata {
  private Long lastModifiedSeconds;
  private Integer lastModifiedNanos;
  private Long sizeBytes;
  private String location;

  public WalFileMetadata(
      Long lastModifiedSeconds, Integer lastModifiedNanos, Long sizeBytes, String location) {

    this.lastModifiedSeconds = lastModifiedSeconds;

    this.lastModifiedNanos = lastModifiedNanos;

    this.sizeBytes = sizeBytes;

    this.location = location;
  }

  public Long lastModifiedSeconds() {
    return this.lastModifiedSeconds;
  }

  public Integer lastModifiedNanos() {
    return this.lastModifiedNanos;
  }

  public Long sizeBytes() {
    return this.sizeBytes;
  }

  public String location() {
    return this.location;
  }

  public void setLastModifiedSeconds(Long lastModifiedSeconds) {
    this.lastModifiedSeconds = lastModifiedSeconds;
  }

  public void setLastModifiedNanos(Integer lastModifiedNanos) {
    this.lastModifiedNanos = lastModifiedNanos;
  }

  public void setSizeBytes(Long sizeBytes) {
    this.sizeBytes = sizeBytes;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WalFileMetadata) {
      WalFileMetadata t = (WalFileMetadata) other;
      return (Objects.equals(lastModifiedSeconds, t.lastModifiedSeconds)
          && Objects.equals(lastModifiedNanos, t.lastModifiedNanos)
          && Objects.equals(sizeBytes, t.sizeBytes)
          && Objects.equals(location, t.location));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastModifiedSeconds, lastModifiedNanos, sizeBytes, location);
  }
}
