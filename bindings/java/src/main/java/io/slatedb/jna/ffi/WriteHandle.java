package io.slatedb.jna.ffi;

import java.util.Objects;

public class WriteHandle {
  private Long seqnum;
  private Long createTs;

  public WriteHandle(Long seqnum, Long createTs) {

    this.seqnum = seqnum;

    this.createTs = createTs;
  }

  public Long seqnum() {
    return this.seqnum;
  }

  public Long createTs() {
    return this.createTs;
  }

  public void setSeqnum(Long seqnum) {
    this.seqnum = seqnum;
  }

  public void setCreateTs(Long createTs) {
    this.createTs = createTs;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WriteHandle) {
      WriteHandle t = (WriteHandle) other;
      return (Objects.equals(seqnum, t.seqnum) && Objects.equals(createTs, t.createTs));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(seqnum, createTs);
  }
}
