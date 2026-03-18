package io.slatedb.jna.ffi;

import java.util.Objects;

public class KeyValue {
  private byte[] key;
  private byte[] value;
  private Long seq;
  private Long createTs;
  private Long expireTs;

  public KeyValue(byte[] key, byte[] value, Long seq, Long createTs, Long expireTs) {

    this.key = key;

    this.value = value;

    this.seq = seq;

    this.createTs = createTs;

    this.expireTs = expireTs;
  }

  public byte[] key() {
    return this.key;
  }

  public byte[] value() {
    return this.value;
  }

  public Long seq() {
    return this.seq;
  }

  public Long createTs() {
    return this.createTs;
  }

  public Long expireTs() {
    return this.expireTs;
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public void setSeq(Long seq) {
    this.seq = seq;
  }

  public void setCreateTs(Long createTs) {
    this.createTs = createTs;
  }

  public void setExpireTs(Long expireTs) {
    this.expireTs = expireTs;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof KeyValue) {
      KeyValue t = (KeyValue) other;
      return (Objects.equals(key, t.key)
          && Objects.equals(value, t.value)
          && Objects.equals(seq, t.seq)
          && Objects.equals(createTs, t.createTs)
          && Objects.equals(expireTs, t.expireTs));
    }
    ;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, seq, createTs, expireTs);
  }
}
