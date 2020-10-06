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
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.DatalogRecordType;
import io.warp10.continuum.store.thrift.data.Metadata;

/**
 * This class contains helper methods to create WAL records.
 */
public class DatalogHelper {
  
  private static final String id;
  
  static {
    id = WarpConfig.getProperty(Configuration.DATALOG_ID);
    
    if (null == id) {
      throw new RuntimeException("Datalog id MUST be set via '" + Configuration.DATALOG_ID + "'.");
    }
  }
  
  public static DatalogRecord getUpdateRecord(GTSEncoder encoder) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.UPDATE);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(encoder.getMetadata());
    record.setBaseTimestamp(encoder.getBaseTimestamp());
    record.setEncoder(encoder.getBytes());
    
    return record;
  }
  
  public static DatalogRecord getDeleteRecord(Metadata metadata, long start, long end) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.DELETE);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(metadata);
    record.setStart(start);
    record.setStop(end);

    return record;
  }
  
  public static DatalogRecord getRegisterRecord(Metadata metadata) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.REGISTER);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(metadata);
    
    return record;
  }
  
  public static DatalogRecord getUnregisterRecord(Metadata metadata) throws IOException {
    DatalogRecord record = new DatalogRecord();
    record.setType(DatalogRecordType.UNREGISTER);
    record.setId(id);
    record.setTimestamp(System.currentTimeMillis());
    record.setMetadata(metadata);
    
    return record;
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
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      deserializer.deserialize(msg, bytes);
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
    int len = in.read(bytes);
    if (bytes.length != len) {
      throw new IOException("Invalid length, expected " + size + " got " + len);
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
}
