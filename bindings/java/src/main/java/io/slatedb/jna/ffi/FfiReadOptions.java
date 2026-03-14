package io.slatedb.jna.ffi;

import java.util.Objects;

public class FfiReadOptions {
  private FfiDurabilityLevel durabilityFilter;
  private Boolean dirty;
  private Boolean cacheBlocks;

  public FfiReadOptions(FfiDurabilityLevel durabilityFilter, Boolean dirty, Boolean cacheBlocks) {

    this.durabilityFilter = durabilityFilter;

    this.dirty = dirty;

    this.cacheBlocks = cacheBlocks;
  }

  public FfiDurabilityLevel durabilityFilter() {
    return this.durabilityFilter;
  }

  public Boolean dirty() {
    return this.dirty;
  }

  public Boolean cacheBlocks() {
    return this.cacheBlocks;
  }

  public void setDurabilityFilter(FfiDurabilityLevel durabilityFilter) {
    this.durabilityFilter = durabilityFilter;
  }

  public void setDirty(Boolean dirty) {
    this.dirty = dirty;
  }

  public void setCacheBlocks(Boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FfiReadOptions) {
      FfiReadOptions t = (FfiReadOptions) other;
      return (Objects.equals(durabilityFilter, t.durabilityFilter)
          && Objects.equals(dirty, t.dirty)
          && Objects.equals(cacheBlocks, t.cacheBlocks));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(durabilityFilter, dirty, cacheBlocks);
  }
}
