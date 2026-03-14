package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiScanOptions {
  private FfiDurabilityLevel durabilityFilter;
  private Boolean dirty;
  private Long readAheadBytes;
  private Boolean cacheBlocks;
  private Long maxFetchTasks;

  public FfiScanOptions(
      FfiDurabilityLevel durabilityFilter,
      Boolean dirty,
      Long readAheadBytes,
      Boolean cacheBlocks,
      Long maxFetchTasks) {

    this.durabilityFilter = durabilityFilter;

    this.dirty = dirty;

    this.readAheadBytes = readAheadBytes;

    this.cacheBlocks = cacheBlocks;

    this.maxFetchTasks = maxFetchTasks;
  }

  public FfiDurabilityLevel durabilityFilter() {
    return this.durabilityFilter;
  }

  public Boolean dirty() {
    return this.dirty;
  }

  public Long readAheadBytes() {
    return this.readAheadBytes;
  }

  public Boolean cacheBlocks() {
    return this.cacheBlocks;
  }

  public Long maxFetchTasks() {
    return this.maxFetchTasks;
  }

  public void setDurabilityFilter(FfiDurabilityLevel durabilityFilter) {
    this.durabilityFilter = durabilityFilter;
  }

  public void setDirty(Boolean dirty) {
    this.dirty = dirty;
  }

  public void setReadAheadBytes(Long readAheadBytes) {
    this.readAheadBytes = readAheadBytes;
  }

  public void setCacheBlocks(Boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
  }

  public void setMaxFetchTasks(Long maxFetchTasks) {
    this.maxFetchTasks = maxFetchTasks;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiScanOptions) {
      FfiScanOptions t = (FfiScanOptions) other;
      return (Objects.equals(durabilityFilter, t.durabilityFilter)
          && Objects.equals(dirty, t.dirty)
          && Objects.equals(readAheadBytes, t.readAheadBytes)
          && Objects.equals(cacheBlocks, t.cacheBlocks)
          && Objects.equals(maxFetchTasks, t.maxFetchTasks));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(durabilityFilter, dirty, readAheadBytes, cacheBlocks, maxFetchTasks);
  }
}
