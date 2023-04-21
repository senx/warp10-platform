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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbLock;
import org.iq80.leveldb.impl.Filename;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.InternalUserComparator;
import org.iq80.leveldb.impl.TableCache;
import org.iq80.leveldb.impl.VersionEdit;
import org.iq80.leveldb.impl.VersionSet;
import org.iq80.leveldb.table.BytewiseComparator;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class WarpPurge {
  public static void main(String[] args) throws Exception {
    String path = args[0];

    List<Long> sstfiles = new ArrayList<Long>();

    if (args.length > 1) {
      for (int i = 1; i < args.length; i++) {
        sstfiles.add(Long.parseLong(args[i]));
      }
    } else {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

      while (true) {
        String line = br.readLine();

        if (null == line) {
          break;
        }

        sstfiles.add(Long.parseLong(line.trim()));
      }

      br.close();
    }

    List<Long> deleted = purge(path, sstfiles);

    for (long id: deleted) {
      System.out.println(id);
    }
  }

  public static List<Long> purge(String leveldbhome, List<Long> sstfiles) throws Exception {

    DbLock dbLock = null;

    List<Long> deleted = new ArrayList<Long>();

    try {
      Options options = new Options();
      File home = new File(leveldbhome);
      dbLock = new DbLock(new File(home, Filename.lockFileName()));

      InternalKeyComparator internalKeyComparator = new InternalKeyComparator(new BytewiseComparator());

      // Reserve ten files or so for other uses and give the rest to TableCache.
      int tableCacheSize = options.maxOpenFiles() - 10;
      TableCache tableCache = new TableCache(home, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());

      VersionSet versions = new VersionSet(home, tableCache, internalKeyComparator);

      // load  (and recover) current version
      versions.recover();

      //
      // Get report
      //

      File currentFile = new File(home, Filename.currentFileName());
      Preconditions.checkState(currentFile.exists(), "CURRENT file does not exist");

      String currentName = Files.toString(currentFile, Charsets.UTF_8);
      if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
        throw new IllegalStateException("CURRENT file does not end with newline");
      }
      currentName = currentName.substring(0, currentName.length() - 1);

      Map<String, Object> report = WarpReport.report(new File(home, currentName).getAbsolutePath());

      int maxlevel = ((Number) report.get(WarpReport.MAXLEVEL_KEY)).intValue();

      VersionEdit edit = new VersionEdit();

      int count = 0;

      for (List<Object> entry: (List<List<Object>>) report.get(WarpReport.SST_KEY)) {

        int level = ((Number) entry.get(0)).intValue();
        long number = ((Number) entry.get(1)).longValue();

        //
        // We only support deleting SST files which are at the deepest level for now
        // Deleting files which are at shallower levels must be carefully planned as
        // they might contain datapoints which override some datapoints at deeper levels.
        // We could chose to let any SST file be deleted, leaving the responsability for
        // careful checking to the caller. For now we only support deleting at maxlevel.
        //
        if (maxlevel == level && sstfiles.contains(number)) {
          edit.deleteFile(level, number);
          deleted.add(number);
          count++;
        }

        if (count >= 1000) {
          versions.logAndApply(edit);
          edit = new VersionEdit();
          count = 0;
        }
      }

      if (count > 0) {
        versions.logAndApply(edit);
      }

      versions.destroy();
    } finally {
      if (null != dbLock) {
        dbLock.release();
      }
    }

    return deleted;
  }
}
