//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10.leveldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Hex;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.LogReader;
import org.iq80.leveldb.impl.VersionEdit;
import org.iq80.leveldb.util.Slice;

import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.SNAPSHOT;

/**
 * Reads a MANIFEST file and outputs a report about each .sst file
 */
public class WarpReport {
  public static final String MAXLEVEL_KEY = "maxlevel";
  public static final String SST_KEY = "sst";
  public static final String MANIFEST = "manifest";
  public static final String LOG_KEY = "log";

  public static boolean debug = false;

  public static Map<String, Object> report(String manifest) throws IOException {

    FileChannel channel = null;
    FileInputStream is = null;

    File path = new File(manifest);

    try {
      is = new FileInputStream(path);
      channel = is.getChannel();

      LogReader reader = new LogReader(channel, null, true, 0);

      Map<Long, FileMetaData> files = new HashMap<Long, FileMetaData>();
      Map<Long, Integer> levels = new HashMap<Long, Integer>();

      Long lognumber = null;

      while (true) {
        Slice slice = reader.readRecord();

        if (null == slice) {
          break;
        }

        VersionEdit edit = new VersionEdit(slice);

        lognumber = edit.getLogNumber();

        if (debug) {
          System.out.println(edit);
        }

        // Ignore compaction pointers and deleted files

        for (Entry<Integer, Long> entry: edit.getDeletedFiles().entries()) {
          files.remove(entry.getValue());
          levels.remove(entry.getValue());
        }

        // Report current files
        for (Entry<Integer, FileMetaData> entry: edit.getNewFiles().entries()) {
          Integer level = entry.getKey();

          files.put(entry.getValue().getNumber(), entry.getValue());
          levels.put(entry.getValue().getNumber(), level);
        }

      }

      int maxlevel = 0;

      List<Object> sstentries = new ArrayList<Object>();

      for (Entry<Long, FileMetaData> entry: files.entrySet()) {

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

        List<Object> sstentry = new ArrayList<Object>();

        sstentry.add((long) level);
        sstentry.add(fileMetaData.getNumber());
        sstentry.add(smallest);
        sstentry.add(largest);

        sstentries.add(sstentry);
      }

      Map<String, Object> result = new HashMap<String, Object>();

      result.put(WarpReport.MAXLEVEL_KEY, (long) maxlevel);
      result.put(WarpReport.SST_KEY, sstentries);
      result.put(WarpReport.MANIFEST, path.getName());
      result.put(WarpReport.LOG_KEY, lognumber);

      return result;
    } catch (FileNotFoundException fnfe) {
      throw new IOException("MANIFEST file was not found.");
    } finally {
      if (null != is) {
        is.close();
      }
      if (null != channel) {
        channel.close();
      }
    }
  }

  public static void main(String[] args) throws IOException, WarpScriptException {

    debug = null != System.getProperty("debug");

    if (args.length != 1) {
      System.err.println("Missing MANIFEST file.");
      System.exit(-1);
    }

    Map<String, Object> report = report(args[0]);

    StringBuilder sb = new StringBuilder();

    SNAPSHOT.addElement(sb, report);

    System.out.println(sb);

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
  }
}
