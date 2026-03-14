package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiReaderOptions {
  private Long manifestPollIntervalMs;
  private Long checkpointLifetimeMs;
  private Long maxMemtableBytes;
  private Boolean skipWalReplay;

  public FfiReaderOptions(
      Long manifestPollIntervalMs,
      Long checkpointLifetimeMs,
      Long maxMemtableBytes,
      Boolean skipWalReplay) {

    this.manifestPollIntervalMs = manifestPollIntervalMs;

    this.checkpointLifetimeMs = checkpointLifetimeMs;

    this.maxMemtableBytes = maxMemtableBytes;

    this.skipWalReplay = skipWalReplay;
  }

  public Long manifestPollIntervalMs() {
    return this.manifestPollIntervalMs;
  }

  public Long checkpointLifetimeMs() {
    return this.checkpointLifetimeMs;
  }

  public Long maxMemtableBytes() {
    return this.maxMemtableBytes;
  }

  public Boolean skipWalReplay() {
    return this.skipWalReplay;
  }

  public void setManifestPollIntervalMs(Long manifestPollIntervalMs) {
    this.manifestPollIntervalMs = manifestPollIntervalMs;
  }

  public void setCheckpointLifetimeMs(Long checkpointLifetimeMs) {
    this.checkpointLifetimeMs = checkpointLifetimeMs;
  }

  public void setMaxMemtableBytes(Long maxMemtableBytes) {
    this.maxMemtableBytes = maxMemtableBytes;
  }

  public void setSkipWalReplay(Boolean skipWalReplay) {
    this.skipWalReplay = skipWalReplay;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiReaderOptions) {
      FfiReaderOptions t = (FfiReaderOptions) other;
      return (Objects.equals(manifestPollIntervalMs, t.manifestPollIntervalMs)
          && Objects.equals(checkpointLifetimeMs, t.checkpointLifetimeMs)
          && Objects.equals(maxMemtableBytes, t.maxMemtableBytes)
          && Objects.equals(skipWalReplay, t.skipWalReplay));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        manifestPollIntervalMs, checkpointLifetimeMs, maxMemtableBytes, skipWalReplay);
  }
}
