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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRecord;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandaloneDirectoryClient;

public class FileBasedDatalogManager extends DatalogManager implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedDatalogManager.class);

  public static final String SUFFIX = ".datalog";

  private static final long DEFAULT_MAXSIZE = 128 * 1024 * 1024L;
  private static final long DEFAULT_MAXTIME = 600 * 1000L;
  private static final long DEFAULT_PURGE = 0L;

  public static final String CONFIG_DATALOG_MANAGER_DIR = "datalog.manager.dir";
  public static final String CONFIG_DATALOG_MANAGER_MAXSIZE = "datalog.manager.maxsize";
  public static final String CONFIG_DATALOG_MANAGER_MAXTIME = "datalog.manager.maxtime";
  public static final String CONFIG_DATALOG_MANAGER_PURGE = "datalog.manager.purge";
  public static final String CONFIG_DATALOG_MANAGER_SYNCALL = "datalog.manager.syncall";
  public static final String CONFIG_DATALOG_MANAGER_COMPRESS = "datalog.manager.compress";
  public static final String CONFIG_DATALOG_MANAGER_ID = "datalog.manager.id";
  public static final String CONFIG_DATALOG_MANAGER_NOFORWARD = "datalog.manager.noforward";
  public static final String CONFIG_DATALOG_MANAGER_FORWARD = "datalog.manager.forward";
  public static final String CONFIG_DATALOG_MANAGER_LOGUPDATES = "datalog.manager.logupdates";
  public static final String CONFIG_DATALOG_MANAGER_LOGDELETES = "datalog.manager.logdeletes";

  public static final String CONFIG_DATALOG_FEEDER_ID = "datalog.feeder.id";
  public static final String CONFIG_DATALOG_FEEDER_HOST = "datalog.feeder.host";
  public static final String CONFIG_DATALOG_FEEDER_PORT = "datalog.feeder.port";
  public static final String CONFIG_DATALOG_FEEDER_MAXCLIENTS = "datalog.feeder.maxclients";
  public static final String CONFIG_DATALOG_FEEDER_BACKLOG = "datalog.feeder.backlog";
  public static final String CONFIG_DATALOG_FEEDER_MAXSIZE = "datalog.feeder.maxsize";
  public static final String CONFIG_DATALOG_FEEDER_INFLIGHT = "datalog.feeder.inflight";
  public static final String CONFIG_DATALOG_FEEDER_TIMEOUT = "datalog.feeder.timeout";
  public static final String CONFIG_DATALOG_FEEDER_DIR = "datalog.feeder.dir";
  public static final String CONFIG_DATALOG_FEEDER_ECC_PRIVATE = "datalog.feeder.ecc.private";
  public static final String CONFIG_DATALOG_FEEDER_ENCRYPT = "datalog.feeder.encrypt";
  public static final String CONFIG_DATALOG_FEEDER_CHECKMACRO = "datalog.feeder.checkmacro";

  public static final String CONFIG_DATALOG_CONSUMER_ECC_PRIVATE = "datalog.consumer.ecc.private";
  public static final String CONFIG_DATALOG_CONSUMER_FEEDER_ECC_PUBLIC = "datalog.consumer.feeder.ecc.public";
  public static final String CONFIG_DATALOG_CONSUMER_ID = "datalog.consumer.id";
  public static final String CONFIG_DATALOG_CONSUMER_EXCLUDED = "datalog.consumer.excluded";
  public static final String CONFIG_DATALOG_CONSUMER_FEEDER_HOST = "datalog.consumer.feeder.host";
  public static final String CONFIG_DATALOG_CONSUMER_FEEDER_PORT = "datalog.consumer.feeder.port";
  public static final String CONFIG_DATALOG_CONSUMER_FEEDER_SHARDS = "datalog.consumer.feeder.shards";
  public static final String CONFIG_DATALOG_CONSUMER_FEEDER_SHARDSHIFT = "datalog.consumer.feeder.shardshift";
  public static final String CONFIG_DATALOG_CONSUMER_SHARDS = "datalog.consumer.shards";
  public static final String CONFIG_DATALOG_CONSUMER_SHARDSHIFT = "datalog.consumer.shardshift";
  public static final String CONFIG_DATALOG_CONSUMER_OFFSETFILE = "datalog.consumer.offsetfile";
  public static final String CONFIG_DATALOG_CONSUMER_OFFSETDELAY = "datalog.consumer.offsetdelay";
  public static final String CONFIG_DATALOG_CONSUMER_MACRO = "datalog.consumer.macro";
  public static final String CONFIG_DATALOG_CONSUMER_MACRO_DATA = "datalog.consumer.macro.data";
  public static final String CONFIG_DATALOG_CONSUMER_MACRO_TOKEN = "datalog.consumer.macro.token";
  // The following configuration are global to all consumers
  public static final String CONFIG_DATALOG_CONSUMER_FLUSH_INTERVAL = "datalog.consumer.flush.interval";
  public static final String CONFIG_DATALOG_CONSUMER_REGISTER_UPDATES = "datalog.consumer.register.updates";
  public static final String CONFIG_DATALOG_CONSUMER_NWORKERS = "datalog.consumer.nworkers";

  public static final String SF_META_NOW = "now";
  public static final String SF_META_UUID = "uuid";
  public static final String SF_META_ID = "id";

  private final AtomicBoolean done = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong size = new AtomicLong(0L);
  private final ReentrantLock lock = new ReentrantLock(true);
  private final AtomicLong start = new AtomicLong(0L);

  /**
   * Current SequenceFile Writer
   */
  private SequenceFile.Writer datalog = null;
  private final FileSystem fs;
  private final Path dirpath;
  private final long MAXSIZE;
  private final long MAXTIME;
  private final long PURGE_DELAY;
  private final Configuration conf;
  private final Matcher DATALOG_MATCHER = Pattern.compile("^(?<ts>[0-9a-fA-F]{16}).(?<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})" + SUFFIX.replaceAll("\\.",  "\\\\.") + "$").matcher("");

  private final boolean syncAll;
  private final boolean compress;
  private final String id;

  /**
   * Path of current SequenceFile
   */
  private Path datalogpath = null;
  private long lastpurge = 0L;

  private StoreClient storeClient = null;
  private StandaloneDirectoryClient directoryClient = null;

  /**
   * List of all active SequenceFile
   */
  private List<String> activeFiles = new ArrayList<String>();

  /**
   * Set of ids that will be forwarded in 'process'
   */
  private final Set<String> forward;

  /**
   * Set of ids that will NOT be forwarded in 'process'
   */
  private final Set<String> noforward;

  /**
   * Should we log data update/delete records (set to false when in standalone+)
   */
  private final boolean logupdates;
  private final boolean logdeletes;

  private static final boolean REGISTER_UPDATES;

  static {
    REGISTER_UPDATES = "true".equals(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_CONSUMER_REGISTER_UPDATES));
  }

  public FileBasedDatalogManager() {

    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    String dirname = WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_DIR);

    try {
      fs = FileSystem.get(URI.create(dirname), conf);
      // We need to disable write checksums, otherwise a call to hsync will
      // NOT sync to disk!
      fs.setWriteChecksum(false);
    } catch (IOException ioe) {
      throw new RuntimeException("Error getting Datalog filesystem.");
    }

    if (null == dirname) {
      throw new RuntimeException("Missing '" + CONFIG_DATALOG_MANAGER_DIR + "' configuration.");
    }

    dirpath = new Path(dirname);

    try {
      if (!fs.exists(dirpath) || !fs.isDirectory(dirpath)) {
        throw new RuntimeException("Invalid '" + CONFIG_DATALOG_MANAGER_DIR + "' " + dirpath);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Error accessing " + dirpath);
    }

    MAXSIZE = Long.parseLong(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_MAXSIZE, Long.toString(DEFAULT_MAXSIZE)));
    MAXTIME = Long.parseLong(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_MAXTIME, Long.toString(DEFAULT_MAXTIME)));
    PURGE_DELAY = Long.parseLong(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_PURGE, Long.toString(DEFAULT_PURGE)));

    // Do we sync to disk after each individual records?
    syncAll = "true".equals(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_SYNCALL));

    // Are SequenceFiles compressed?
    compress = "true".equals(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_COMPRESS));

    // Our unique id
    id = WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_ID);

    if (null == id) {
      throw new RuntimeException("Missing Datalog id '" + CONFIG_DATALOG_MANAGER_ID + "'.");
    }

    // Should we log UPDATE / DELETE records?
    this.logupdates = "true".equals(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_LOGUPDATES, "true"));
    this.logdeletes = "true".equals(WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_LOGDELETES, "true"));

    if (null != WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_FORWARD)) {
      String[] ids =  WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_FORWARD).split(",");
      forward = new LinkedHashSet<String>();
      for (String id: ids) {
        id = id.trim();
        if ("".equals(id)) {
          continue;
        }
        forward.add(id);
      }
    } else {
      forward = null;
    }

    if (null != WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_NOFORWARD)) {
      String[] ids =  WarpConfig.getProperty(CONFIG_DATALOG_MANAGER_NOFORWARD).split(",");
      noforward = new LinkedHashSet<String>();
      for (String id: ids) {
        id = id.trim();
        if ("".equals(id)) {
          continue;
        }
        noforward.add(id);
      }
    } else {
      noforward = null;
    }

    //
    // Create shutdown hook
    //

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        done.set(true);
        while(!closed.get()) {
          LockSupport.parkNanos(1000000000L);
        }
      }
    });

    //
    // Populate the list of active files
    //

    try {
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles(dirpath, false);

      while(iter.hasNext()) {
        LocatedFileStatus status = iter.next();
        String name = status.getPath().getName();

        DATALOG_MATCHER.reset(name);
        if (DATALOG_MATCHER.matches()) {
          activeFiles.add(name);
        }
      }

      Collections.sort(activeFiles);
    } catch (IOException ioe) {
      throw new RuntimeException("Unable to list active Datalog files.", ioe);
    }

    //
    // Create the flushing thread. This thread keeps track of the size and time interval of each
    // SequenceFile and opens a new one if a threshold is reached.
    //

    Thread flusher = new Thread(this);
    flusher.setDaemon(true);
    flusher.setName("[Datalog Flusher]");
    flusher.start();
    LOG.info("Started Datalog manager using directory '" + dirpath + "'.");

    new TCPDatalogFeeder(this);
  }

  @Override
  protected void register(Metadata metadata) throws IOException {
    //System.out.println("register: " + metadata);
    if (null == metadata) {
      append(null);
    } else {
      append(DatalogHelper.getRegisterRecord(id, metadata));
    }
  }

  @Override
  protected void unregister(Metadata metadata) throws IOException {
    //System.out.println("unregister: " + metadata);
    if (null == metadata) {
      append(null);
    } else {
      append(DatalogHelper.getUnregisterRecord(id, metadata));
    }
  }

  @Override
  protected void store(GTSEncoder encoder) throws IOException {
    //System.out.println("data store: " + encoder);

    if (!logupdates) {
      return;
    }

    if (null == encoder) {
      append(null);
    } else {
      append(DatalogHelper.getUpdateRecord(id, encoder));
    }
  }

  @Override
  protected void delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    if (!logdeletes) {
      return;
    }
    // Do nothing if Metadata is null, this is simply a marker to instruct some StoreClient (namely FDB) to
    // flush their mutations
    if (null == metadata) {
      return;
    }
    System.out.println("delete: " + token + " " + metadata + " " + start + " " + end);
    append(DatalogHelper.getDeleteRecord(id, token, metadata, start, end));
  }

  @Override
  protected void process(DatalogRecord record) throws IOException {

    // a null record is a special marker meant to trigger a flush
    if (null == record) {
      this.storeClient.store(null);
      this.storeClient.delete(null, null, Long.MAX_VALUE, Long.MAX_VALUE);
      this.directoryClient.unregister(null);
      this.directoryClient.register(null);
      return;
    }

    //
    // Ignore the record if it was created by this instance
    // to avoid loops.
    //

    if (this.id.equals(record.getId())) {
      return;
    }

    switch (record.getType()) {
      case UPDATE:
        GTSDecoder decoder = new GTSDecoder(record.getBaseTimestamp(), record.bufferForEncoder());
        decoder.next();
        GTSEncoder encoder = decoder.getEncoder();
        encoder.setMetadata(record.getMetadata());
        WriteToken token = null;
        try {
          token = (WriteToken) WarpConfig.getThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN);
          WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN, record.getToken());
          this.storeClient.store(encoder);
          if (REGISTER_UPDATES) {
            encoder.getMetadata().setSource(io.warp10.continuum.Configuration.INGRESS_METADATA_SOURCE);
            this.directoryClient.register(encoder.getMetadata());
          }
        } finally {
          if (null != token) {
            WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN, token);
          } else {
            WarpConfig.removeThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN);
          }
        }
        break;

      case REGISTER:
        this.directoryClient.register(record.getMetadata());
        break;

      case UNREGISTER:
        this.directoryClient.unregister(record.getMetadata());
        break;

      case DELETE:
        this.storeClient.delete(record.getToken(), record.getMetadata(), record.getStart(), record.getStop());
        break;
    }

    // If the record id is in 'noforward', don't append the record to the log file
    if (null != noforward && noforward.contains(record.getId())) {
      return;
    }

    // If a 'forward' list was defined and the id is not in it, don't append the record to the log file
    if (null != forward && !forward.contains(record.getId())) {
      return;
    }

    append(record);
  }

  @Override
  protected StandaloneDirectoryClient wrap(StandaloneDirectoryClient sdc) {
    if (null != this.directoryClient) {
      throw new RuntimeException("DirectoryClient already set.");
    }
    this.directoryClient = sdc;
    return new DatalogStandaloneDirectoryClient(this, sdc);
  }

  @Override
  protected StoreClient wrap(StoreClient sc) {
    if (null != this.storeClient) {
      throw new RuntimeException("StoreClient already set.");
    }
    this.storeClient = sc;
    return new DatalogStoreClient(this, sc);
  }

  private void purge() {
    long now = System.currentTimeMillis();
    if (PURGE_DELAY > 0 && now - lastpurge > Math.min(MAXTIME, PURGE_DELAY)) {
      // Retrieve all Datalog files
      try {
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(dirpath, false);

        // Any file whose timestamp appearing in the name is
        // earlier than PURGE_DELAY + 2 * MAXTIME ago will be retained,
        // others will be purged

        long cutoff = now - (PURGE_DELAY + 2 * MAXTIME);

        // Iterate over the files
        while(iter.hasNext()) {
          LocatedFileStatus status = iter.next();
          String name = status.getPath().getName();

          // Ignore the current file
          if (null != datalogpath && name.equals(datalogpath.getName())) {
            continue;
          }

          DATALOG_MATCHER.reset(name);
          if (DATALOG_MATCHER.matches()) {
            long ts = Long.parseLong(DATALOG_MATCHER.group("ts"), 16);
            // If the file was created after the cutoff timestamp, ignore it
            if (ts >= cutoff) {
              continue;
            }
            // Delete the file
            try {
              fs.delete(status.getPath(), false);
              activeFiles.remove(name);
            } catch (IOException ioe) {
            }
          }
        }
        lastpurge = now;
      } catch (IOException ioe) {
        LOG.error("Unable to perform Datalog purge", ioe);
      }
    }
  }

  @Override
  protected void replay(StandaloneDirectoryClient sdc, StoreClient scc) {
    //
    // Purge Datalog first
    //
    purge();

    //
    // List all files
    //

    try {
      lock.lockInterruptibly();

      FileStatus[] files = fs.listStatus(dirpath);

      // Sort by name
      Arrays.sort(files, new Comparator<FileStatus>() {
        @Override
        public int compare(FileStatus o1, FileStatus o2) {
          return o1.getPath().getName().compareTo(o2.getPath().getName());
        }
      });

      for (FileStatus file: files) {
        // Only consider files which match DATALOG_MATCHER
        if (!DATALOG_MATCHER.reset(file.getPath().getName()).matches()) {
          continue;
        }
        LOG.info("Replaying " + file.getPath());
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(file.getPath()),
          SequenceFile.Reader.start(0));

        BytesWritable key = new BytesWritable();
        BytesWritable val = new BytesWritable();

        long count = 0L;

        while(reader.next(key, val)) {
          System.out.println("VAL=" + DatalogHelper.getRecord(val.getBytes(), 0, val.getLength()));
          count++;
        }

        LOG.info("Replayed " + count + " records.");
        reader.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error replaying Datalog.", e);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
  }

  /**
   *
   * @param record to store, will be modified by the method calling DetalogHelper.serialize(record)
   * @throws IOException
   */
  private void append(DatalogRecord record) throws IOException {
    try {
      lock.lockInterruptibly();
      if (null == datalog) {
        throw new IOException("Datalog is not ready.");
      }

      if (null != record) {
        // Set the store timestamp. This guarantees it will be past
        // the timestamp of the DatalogFile
        record.setStoreTimestamp(System.currentTimeMillis());
        byte[] value = DatalogHelper.serialize(record);

        // Key is store timestamp + class id + labels id (to ease sharding)
        byte[] key = new byte[24];

        int offset = 0;
        long l = record.getStoreTimestamp();
        key[offset++] = (byte) ((l >>> 56) & 0xFFL);
        key[offset++] = (byte) ((l >>> 48) & 0xFFL);
        key[offset++] = (byte) ((l >>> 40) & 0xFFL);
        key[offset++] = (byte) ((l >>> 32) & 0xFFL);
        key[offset++] = (byte) ((l >>> 24) & 0xFFL);
        key[offset++] = (byte) ((l >>> 16) & 0xFFL);
        key[offset++] = (byte) ((l >>> 8) & 0xFFL);
        key[offset++] = (byte) (l & 0xFFL);

        l = record.getMetadata().getClassId();
        key[offset++] = (byte) ((l >>> 56) & 0xFFL);
        key[offset++] = (byte) ((l >>> 48) & 0xFFL);
        key[offset++] = (byte) ((l >>> 40) & 0xFFL);
        key[offset++] = (byte) ((l >>> 32) & 0xFFL);
        key[offset++] = (byte) ((l >>> 24) & 0xFFL);
        key[offset++] = (byte) ((l >>> 16) & 0xFFL);
        key[offset++] = (byte) ((l >>> 8) & 0xFFL);
        key[offset++] = (byte) (l & 0xFFL);

        l = record.getMetadata().getLabelsId();
        key[offset++] = (byte) ((l >>> 56) & 0xFFL);
        key[offset++] = (byte) ((l >>> 48) & 0xFFL);
        key[offset++] = (byte) ((l >>> 40) & 0xFFL);
        key[offset++] = (byte) ((l >>> 32) & 0xFFL);
        key[offset++] = (byte) ((l >>> 24) & 0xFFL);
        key[offset++] = (byte) ((l >>> 16) & 0xFFL);
        key[offset++] = (byte) ((l >>> 8) & 0xFFL);
        key[offset++] = (byte) (l & 0xFFL);

        datalog.append(new BytesWritable(key), new BytesWritable(value));
        // Write a sync mark so we can seek to any record based on file position even if file position
        // is between records
        datalog.sync();
        if (syncAll) {
          datalog.hsync();
        }
        size.set(datalog.getLength());
      } else {
        // FIXME(hbs): seems weird
        if (!syncAll) {
          datalog.hsync();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
  }

  @Override
  public void run() {
    try {
      while(true) {
        // Should we close the current file and reopen a new one?
        if (done.get() || null == datalog || size.get() > MAXSIZE || System.currentTimeMillis() - start.get() > MAXTIME) {
          try {
            lock.lockInterruptibly();

            if (null != datalog) {
              datalog.hsync();
              datalog.close();
              if (0 == size.get()) {
                try {
                  fs.delete(datalogpath, false);
                  synchronized(activeFiles) {
                    activeFiles.remove(datalogpath.getName());
                  }
                } catch (Throwable t) {
                }
              }
              datalogpath = null;
              if (done.get()) {
                break;
              }
            }

            long now = System.currentTimeMillis();
            String hexnow = "0000000000000000" + Long.toHexString(now);
            hexnow = hexnow.substring(hexnow.length() - 16);
            String uuid = UUID.randomUUID().toString();
            datalogpath = new Path(dirpath + "/" + hexnow + "." + uuid + SUFFIX);

            org.apache.hadoop.io.SequenceFile.Metadata meta = new org.apache.hadoop.io.SequenceFile.Metadata();
            meta.set(new Text(SF_META_NOW), new Text(String.valueOf(now)));
            meta.set(new Text(SF_META_UUID), new Text(uuid));
            meta.set(new Text(SF_META_ID), new Text(id));

            datalog = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(datalogpath),
                SequenceFile.Writer.keyClass(BytesWritable.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                // If compression should be used, it MUST be record based since we flush after each record
                SequenceFile.Writer.compression(compress ? CompressionType.RECORD : CompressionType.NONE),
                SequenceFile.Writer.metadata(meta)
                );
            size.set(0L);
            start.set(now);
            synchronized(activeFiles) {
              activeFiles.add(datalogpath.getName());
            }
          } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Error opening Datalog file '" + datalogpath + "'.", e);
            if (null != datalog) {
              try { datalog.close(); } catch (Throwable t) {}
            }
          } finally {
            if (lock.isHeldByCurrentThread()) {
              lock.unlock();
            }
          }
        } else {
          purge();
          LockSupport.parkNanos(1000000000L);
        }
      }
    } finally {
      closed.set(true);
    }
  }

  public Map<String,Long> getActiveFiles() {
    return null;
  }

  public String getCurrentFile() {
    if (null == datalogpath) {
      return null;
    }
    return datalogpath.getName();
  }

  /**
   * Return the file after 'key'
   * @param key
   * @return
   */
  public String getNextFile(String key) {
    synchronized(activeFiles) {
      int idx = Collections.binarySearch(activeFiles, key);

      if (idx >= 0) {
        if (idx < activeFiles.size() - 1) {
          // 'key' is in the list and not the last element, so return the
          // element after 'key'
          return activeFiles.get(idx + 1);
        } else {
          // 'key' is the last element, return 'key'
          return activeFiles.get(idx);
        }
      } else {
        // 'key' was not found, return the file following it or null
        // if no file follows.
        idx = -idx - 1;
        if (idx > activeFiles.size() - 1) {
          return null;
        } else {
          return activeFiles.get(idx);
        }
      }
    }
  }

  public String getPreviousFile(String key) {
    synchronized(activeFiles) {
      int idx = Collections.binarySearch(activeFiles, key);

      if (idx >= 0) {
        if (idx > 0) {
          // 'key' is in the list and not the first element, so return the
          // element before 'key'
          return activeFiles.get(idx - 1);
        } else {
          // 'key' is the first element, return null
          return null;
        }
      } else {
        // 'key' was not found, return the file before it or null
        // if no file exists before it.
        idx = -idx - 1;

        if (idx > 0) {
          return activeFiles.get(idx - 1);
        } else {
          return null;
        }
      }
    }
  }
}
