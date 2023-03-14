//
//   Copyright 2022-2023  SenX S.A.S.
//

package io.warp10.fdb;

import java.util.Map.Entry;

import com.apple.foundationdb.KeyValue;

/**
 * A wrapper class which allows for easy stripping of key prefix without
 * allocating new arrays.
 */
public class FDBKeyValue implements Entry<byte[],byte[]> {

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

  @Override
  public byte[] getKey() {
    if (this.kv.getKey().length != klen) {
      byte[] k = new byte[klen];
      System.arraycopy(kv.getKey(), koffset, k, 0, klen);
      return k;
    } else {
      return this.kv.getKey();
    }
  }

  @Override
  public byte[] getValue() {
    if (this.kv.getValue().length != vlen) {
      byte[] v = new byte[vlen];
      System.arraycopy(kv.getValue(), voffset, v, 0, vlen);
      return v;
    } else {
      return this.kv.getValue();
    }
  }

  @Override
  public byte[] setValue(byte[] value) {
    throw new RuntimeException("Not implemented.");
  }
}
