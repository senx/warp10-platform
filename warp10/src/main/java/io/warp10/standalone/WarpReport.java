//
//   Copyright 2018  Cityzen Data
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

package io.warp10.standalone;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.zookeeper.DeletionListener;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.Filename;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.impl.LogReader;
import org.iq80.leveldb.impl.LogWriter;
import org.iq80.leveldb.impl.Logs;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.impl.VersionEdit;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.warp10.continuum.Configuration;

/**
 * Reads a MANIFEST file and outputs a report about each .sst file
 */
public class WarpReport {
  public static void main(String[] args) throws IOException {
    
    if (args.length != 1) {
      System.err.println("Usage: WarpReport /path/to/leveldb/MANIFEST");
      System.exit(-1);
    }
    
    String path = args[0];
    
    FileChannel channel = new FileInputStream(path).getChannel();
   
    LogReader reader = new LogReader(channel, null, true, 0);
        
    Map<Long, FileMetaData> files = new HashMap<Long,FileMetaData>();
    Map<Long, Integer> levels = new HashMap<Long,Integer>();
    
    while(true) {
      Slice slice = reader.readRecord();
      
      if (null == slice) {
        break;
      }
      
      VersionEdit edit = new VersionEdit(slice);
      
      // Ignore compaction pointers and deleted files

      for (Entry<Integer,Long> entry: edit.getDeletedFiles().entries()) {
        files.remove(entry.getValue());
        levels.remove(entry.getValue());
      }
      
      // Report current files
      for (Entry<Integer, FileMetaData> entry : edit.getNewFiles().entries()) {
        Integer level = entry.getKey();
        
        files.put(entry.getValue().getNumber(), entry.getValue());
        levels.put(entry.getValue().getNumber(), level);
      }
    }
    
    int maxlevel = 0;
    
    System.out.println("[]");
    
    for (Entry<Long,FileMetaData> entry: files.entrySet()) {
      
      int level = levels.get(entry.getKey());
      
      if (level > maxlevel) {
        maxlevel = level;
      }
      FileMetaData fileMetaData = entry.getValue();
      int allowedSeeks = (int) (fileMetaData.getFileSize() / 16384);
      if (allowedSeeks < 100) {
          allowedSeeks = 100;
      }
      fileMetaData.setAllowedSeeks(allowedSeeks);

      String smallest = Hex.encodeHexString(fileMetaData.getSmallest().getUserKey().getBytes());
      String largest = Hex.encodeHexString(fileMetaData.getLargest().getUserKey().getBytes());
      System.out.println("[ " + level + " " + fileMetaData.getNumber() + " '" + smallest + "' '" + largest + "' ] +!");
    }      
//          long smallestSeqno = fileMetaData.getSmallest().getSequenceNumber();
//          long largestSeqno = fileMetaData.getLargest().getSequenceNumber();
//          // Create 'empty' sst file with an empty record for the first and last keys
//          
//          LogWriter writer = Logs.createLogWriter(new File("/var/tmp/" + Filename.tableFileName(fileMetaData.getNumber()) + ".empty" + "." + level), fileMetaData.getNumber());
//          
//          // Create empty records for the smallest and largest keys (keeping the sequence numbers)
//          
//          if (smallestSeqno == largestSeqno) {
//            // Both records were written with the same sequence number
//            Slice tslice = Slices.allocate(8 + 4 + 2 + smallest.length() + largest.length());
//            SliceOutput output = tslice.output();
//            output.writeLong(smallestSeqno);
//            output.writeInt(2); // 1 updates
//            //output.writeByte(ValueType.DELETION.getPersistentId());
//            output.writeByte(ValueType.VALUE.getPersistentId());
//            Slices.writeLengthPrefixedBytes(output, fileMetaData.getSmallest().getUserKey());
//            Slices.writeLengthPrefixedBytes(output, new Slice(new byte[0]));
//            //output.writeByte(ValueType.DELETION.getPersistentId());
//            output.writeByte(ValueType.VALUE.getPersistentId());
//            Slices.writeLengthPrefixedBytes(output, fileMetaData.getLargest().getUserKey());
//            Slices.writeLengthPrefixedBytes(output, new Slice(new byte[0]));
//            writer.addRecord(tslice.slice(0,  output.size()), true);
//          } else {
//            Slice tslice = Slices.allocate(8 + 4 + 1 + smallest.length());
//            SliceOutput output = tslice.output();
//            output.writeLong(smallestSeqno);
//            output.writeInt(1); // 1 update
//            //output.writeByte(ValueType.DELETION.getPersistentId());
//            output.writeByte(ValueType.VALUE.getPersistentId());
//            Slices.writeLengthPrefixedBytes(output, fileMetaData.getSmallest().getUserKey());
//            Slices.writeLengthPrefixedBytes(output, new Slice(new byte[0]));
//            writer.addRecord(tslice.slice(0,  output.size()), true);
//            
//            tslice = Slices.allocate(8 + 4 + 1 + largest.length());
//            output = tslice.output();
//            output.writeLong(largestSeqno);
//            output.writeInt(1); // 1 update
//            //output.writeByte(ValueType.DELETION.getPersistentId());
//            output.writeByte(ValueType.VALUE.getPersistentId());
//            Slices.writeLengthPrefixedBytes(output, fileMetaData.getLargest().getUserKey());
//            Slices.writeLengthPrefixedBytes(output, new Slice(new byte[0]));
//            writer.addRecord(tslice.slice(0,  output.size()), true);
//          }
//          
//          writer.close();      
    
    System.out.println(maxlevel + " // max level");
    channel.close();
  }
}
