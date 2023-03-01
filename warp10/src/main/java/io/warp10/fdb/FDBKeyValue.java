package io.warp10.fdb;

import com.apple.foundationdb.KeyValue;

/**
 * A wrapper class which allows for easy stripping of key prefix without
 * allocating new arrays.
 */
public class FDBKeyValue {

  private final int koffset;
  private final int klen;
  private final int voffset;
  private final int vlen;
  private final KeyValue kv;

  public FDBKeyValue(KeyValue kv) {
    this.kv = kv;
    this.koffset = 0;
    this.klen = kv.getKey().length;
    this.voffset = 0;
    this.vlen = kv.getValue().length;
  }

  public FDBKeyValue(KeyValue kv, int koffset, int klen, int voffset, int vlen) {
    this.kv = kv;
    this.koffset = koffset;
    this.klen = klen;
    this.voffset = voffset;
    this.vlen = vlen;
  }

  public byte[] getKeyArray() {
    return this.kv.getKey();
  }

  public int getKeyOffset() {
    return this.koffset;
  }

  public int getKeyLength() {
    return this.klen;
  }

  public byte[] getValueArray() {
    return this.kv.getValue();
  }

  public int getValueOffset() {
    return this.voffset;
  }

  public int getValueLength() {
    return this.vlen;
  }
}
