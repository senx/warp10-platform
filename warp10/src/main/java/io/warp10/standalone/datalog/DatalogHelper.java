//
//   Copyright 2020-2023  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.standalone.datalog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.DatalogRecordType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;

/**
 * This class contains helper methods to create WAL records.
 */
public class DatalogHelper {

  public static DatalogRecord getUpdateRecord(String id, GTSEncoder encoder) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.UPDATE);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(encoder.getMetadata());
    record.setBaseTimestamp(encoder.getBaseTimestamp());
    record.setEncoder(encoder.getBytes());

    WriteToken token = (WriteToken) WarpConfig.getThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN);

    if (null != token) {
      record.setToken(token);
    }

    return record;
  }

  public static DatalogRecord getDeleteRecord(String id, WriteToken token, Metadata metadata, long start, long end) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.DELETE);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(metadata);
    record.setStart(start);
    record.setStop(end);
    record.setToken(token);

    return record;
  }

  public static DatalogRecord getRegisterRecord(String id,Metadata metadata) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.REGISTER);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(metadata);

    return record;
  }

  public static DatalogRecord getUnregisterRecord(String id, Metadata metadata) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.UNREGISTER);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(metadata);

    return record;
  }

  public static byte[] serialize(DatalogRecord record) throws IOException {
    // If 'forward' is set, copy its content to metadata/baseTimestamp/encoder and
    // clear the 'forward' field
    if (record.isSetForward()) {
      record.setMetadata(record.getForward().getMetadata());
      record.setBaseTimestamp(record.getForward().getBase());
      record.setEncoder(record.getForward().getEncoded());
      record.unsetForward();
    }
    return serialize((TBase) record);
  }

  public static byte[] serialize(TBase record) throws IOException {
    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      return serializer.serialize(record);
    } catch (TException te) {
      throw new IOException("Error serializing record.", te);
    }
  }

  public static void deserialize(byte[] bytes, TBase msg) throws IOException {
    deserialize(bytes, 0, bytes.length, msg);
  }

  public static void deserialize(byte[] bytes, int offset, int len, TBase msg) throws IOException {
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      deserializer.deserialize(msg, bytes, offset, len);
    } catch (TException te) {
      throw new IOException("Error deserializing record.", te);
    }
  }

  public static DatalogRecord getRecord(byte[] bytes, int offset, int length) throws IOException {
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      DatalogRecord record = new DatalogRecord();
      deserializer.deserialize(record, bytes, offset, length);
      return record;
    } catch (TException te) {
      throw new IOException("Error deserializing Datalog record.", te);
    }
  }

  static long bytesToLong(byte[] bytes, int offset, int len) throws IOException {

    if (len > 8 || len < 1) {
      throw new IOException("Invalid long len " + len);
    }

    if (len != bytes.length) {
      throw new IOException("Invalid int length, expected " + len + ", got " + bytes.length);
    }

    long value = 0L;

    for (int i = 0; i < len; i++) {
      value <<= 8;
      value |= ((long) bytes[i+offset]) & 0xFFL;
    }

    return value;
  }

  static byte[] readBlob(InputStream in, final int size) throws IOException {
    //
    // Read blob length as big endian 4 bytes int
    //

    byte[] bytes = DatalogHelper.readChunk(in, 4);

    int len = (int) (bytesToLong(bytes,0,4) & 0xFFFFFFFFL);

    if (len > TCPDatalogFeederWorker.MAX_BLOB_SIZE) {
      throw new IOException("Blob size (" + len + ") would exceed max allowed blob size (" + TCPDatalogFeederWorker.MAX_BLOB_SIZE + ").");
    }

    if (0 != size && size != len) {
      throw new IOException("Invalid blob size " + len + ", expected " + size);
    }

    return DatalogHelper.readChunk(in, len);
  }

  static byte[] readChunk(InputStream in, final int size) throws IOException {

    byte[] bytes = new byte[size];

    int len = 0;

    while(len < size) {
      int nread = in.read(bytes, len, size - len);
      if (nread < 0) {
        throw new IOException("EOF reached.");
      }
      len += nread;
    }

    return bytes;
  }

  static void writeLong(OutputStream out, long value, int len) throws IOException {
    if (len < 1 || len > 8) {
      throw new IOException("Invalid length.");
    }

    for (int i = 8 - len; i < 8; i++) {
      out.write((int) ((value >>> (56 - i * 8)) & 0xFFL));
    }
  }

  public static long getShardId(long classid, long labelsid, long shardShift) {
    long shifted = (labelsid >>> shardShift) & 0xFFFFFFFFL;
    shifted |= (classid << (64 - shardShift)) & 0xFFFFFFFFL;
    long sid = shifted;
    return sid;
  }
}
