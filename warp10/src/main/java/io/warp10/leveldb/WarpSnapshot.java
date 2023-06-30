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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.impl.DbLock;
import org.iq80.leveldb.impl.Filename;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class WarpSnapshot {
  public static final String SNAPSHOTS_DIR = "snapshots";

  /**
   * Creates a snapshot of LevelDB
   *
   * @param leveldbhome Directory where LevelDB stores its data files
   * @param snapshotName Name of snapshot to create
   * @param snapshotRef Name of reference snapshot or null if a full snapshot should be created
   * @return A list containing the ids of SST files which still must be linked from the reference snapshot, followed by the number of links already created
   * @throws IOException
   */
  public static List<Long> snapshot(String leveldbhome, String snapshotName, String snapshotRef) throws IOException {

    //
    // Check that the reference snapshot exists
    //

    File snapshothome = new File(leveldbhome, SNAPSHOTS_DIR);
    File refsnapshotdir = null;

    if (null != snapshotRef) {
      refsnapshotdir = new File(snapshothome, snapshotRef);
      if (!refsnapshotdir.exists() || !refsnapshotdir.isDirectory()) {
        throw new IOException("Invalid reference snapshot '" + snapshotRef + "'.");
      }
    }

    //
    // Create the snapshot directory
    //

    File snapshotdir = new File(snapshothome, snapshotName);

    if (snapshotdir.exists()) {
      throw new IOException("Snapshot '" + snapshotName + "' already exists.");
    }

    snapshotdir.mkdirs();

    DbLock dbLock = null;

    try {
      // If doing an incremental snapshot, get the report for the reference snapshot
      // We do that before locking LevelDB so locked time is kept to the bare minimal

      Map<String,Object> refReport = null;

      if (null != snapshotRef) {
        File refCurrentFile = new File(refsnapshotdir, Filename.currentFileName());
        Preconditions.checkState(refCurrentFile.exists(), "Reference snapshot CURRENT file does not exist");

        String refCurrentName = Files.toString(refCurrentFile,  Charsets.UTF_8);
        if (refCurrentName.isEmpty() || refCurrentName.charAt(refCurrentName.length() - 1) != '\n') {
          throw new IllegalStateException("Reference snapshot CURRENT file does not end with newline");
        }
        refCurrentName = refCurrentName.substring(0,  refCurrentName.length() - 1);
        File refmanifest = new File(refsnapshotdir, refCurrentName);
        refReport = WarpReport.report(refmanifest.getAbsolutePath());
      }

      File leveldbrootdir = new File(leveldbhome);
      // Lock LevelDB so we are sure no modification is done while we work
      dbLock = new DbLock(new File(leveldbrootdir, Filename.lockFileName()));

      //
      // Get report
      //

      File currentFile = new File(leveldbrootdir, Filename.currentFileName());
      Preconditions.checkState(currentFile.exists(), "CURRENT file does not exist");

      String currentName = Files.toString(currentFile, Charsets.UTF_8);
      if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
        throw new IllegalStateException("CURRENT file does not end with newline");
      }
      currentName = currentName.substring(0, currentName.length() - 1);
      File manifest = new File(leveldbrootdir, currentName);

      Map<String,Object> leveldbReport = WarpReport.report(manifest.getAbsolutePath());

      //
      // Identify the SST files which are in the current leveldb file set and not in
      // the reference snapshot
      //

      List<List<Object>> sstfiles = (List<List<Object>>) leveldbReport.get(WarpReport.SST_KEY);
      Set<Long> refsstfiles = new HashSet<Long>();

      if (null != snapshotRef) {
        List<List<Object>> refsst = (List<List<Object>>) refReport.get(WarpReport.SST_KEY);
        for (List<Object> sst: refsst) {
          // Store file id
          refsstfiles.add((Long) sst.get(1));
        }
      }

      // Files common to the reference snapshot and the current leveldb SST files
      List<Long> commonsst = new ArrayList<Long>();

      long links = 0;

      for (List<Object> sstfile: sstfiles) {
        long number = (Long) sstfile.get(1);

        if (refsstfiles.contains(number)) {
          commonsst.add(number);
        } else {
          // File is only in the current set of LevelDB files, create a hardlink
          String tableFileName = Filename.tableFileName(number);
          File sst = new File(leveldbrootdir, tableFileName);
          File link = new File(snapshotdir, tableFileName);
          java.nio.file.Files.createLink(link.toPath(), sst.toPath());
          links++;
        }
      }

      //
      // Hard link the CURRENT file and MANIFEST
      //

      java.nio.file.Files.createLink(new File(snapshotdir, currentName).toPath(), manifest.toPath());
      java.nio.file.Files.createLink(new File(snapshotdir, Filename.currentFileName()).toPath(), currentFile.toPath());
      links += 2;

      //
      // Hard link the current log file
      //

      Long lognumber = (Long) leveldbReport.get(WarpReport.LOG_KEY);

      if (null != lognumber) {
        String logfile = Filename.logFileName(lognumber);
        java.nio.file.Files.createLink(new File(snapshotdir, logfile).toPath(), new File(leveldbrootdir, logfile).toPath());
        links++;
      }

      commonsst.add(links);

      return commonsst;
    } finally {
      if (null != dbLock) {
        dbLock.release();
      }
    }
  }
}
