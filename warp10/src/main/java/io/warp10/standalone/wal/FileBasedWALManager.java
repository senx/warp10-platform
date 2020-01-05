package io.warp10.standalone.wal;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandaloneDirectoryClient;
import jline.internal.Log;

public class FileBasedWALManager extends WALManager implements Runnable {
  
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedWALManager.class);
  
  private static final long DEFAULT_MAXSIZE = 128 * 1024 * 1024L;
  private static final long DEFAULT_MAXTIME = 3600 * 1000L;
  private static final long DEFAULT_PURGE = 0L;
  
  public static final String CONFIG_WAL_DIR = "wal.dir";
  public static final String CONFIG_WAL_MAXSIZE = "wal.maxsize";
  public static final String CONFIG_WAL_MAXTIME = "wal.maxtime";
  public static final String CONFIG_WAL_PURGE = "wal.purge";
  
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong size = new AtomicLong(0L);
  private final ReentrantLock lock = new ReentrantLock();
  private final AtomicLong start = new AtomicLong(0L);
  private SequenceFile.Writer wal = null;
  private final FileSystem fs;
  private final Path dirpath;
  private final long MAXSIZE;
  private final long MAXTIME;
  private final long PURGE_DELAY;
  private final Configuration conf;
  private final Matcher WAL_MATCHER = Pattern.compile("^(?<ts>[0-9a-fA-F]{16}).(?<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\\.wal$").matcher("");

  private Path walpath = null;
  private long lastpurge = 0L;

  public FileBasedWALManager() {
    
    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    
    String dirname = WarpConfig.getProperty(CONFIG_WAL_DIR);
    
    try {
      fs = FileSystem.get(URI.create(dirname), conf);
      // We need to disable write checksums, otherwise a call to hsync will
      // NOT sync to disk!
      fs.setWriteChecksum(false);
    } catch (IOException ioe) {
      throw new RuntimeException("Error getting WAL filesystem.");
    }
    
    if (null == dirname) {
      throw new RuntimeException("Missing '" + CONFIG_WAL_DIR + "' configuration.");
    }
    
    dirpath = new Path(dirname);
    
    try {
      if (!fs.exists(dirpath) || !fs.isDirectory(dirpath)) {
        throw new RuntimeException("Invalid '" + CONFIG_WAL_DIR + "' " + dirpath);
      }      
    } catch (IOException ioe) {
      throw new RuntimeException("Error accessing " + dirpath);
    }
    
    MAXSIZE = Long.parseLong(WarpConfig.getProperty(CONFIG_WAL_MAXSIZE, Long.toString(DEFAULT_MAXSIZE)));
    MAXTIME = Long.parseLong(WarpConfig.getProperty(CONFIG_WAL_MAXTIME, Long.toString(DEFAULT_MAXTIME)));
    PURGE_DELAY = Long.parseLong(WarpConfig.getProperty(CONFIG_WAL_PURGE, Long.toString(DEFAULT_PURGE)));
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
    // Create the flushing thread
    //
    
    Thread flusher = new Thread(this);
    flusher.setDaemon(true);
    flusher.setName("[WAL Flusher]");
    flusher.start();
  }
  
  @Override
  protected void register(Metadata metadata) throws IOException {
    System.out.println("register: " + metadata);
    if (null == metadata) {
      append(null);
    } else {
      append(metadata.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  protected void unregister(Metadata metadata) throws IOException {
    System.out.println("unregister: " + metadata);
    if (null == metadata) {
      append(null);
    } else {
      append(metadata.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  protected void store(GTSEncoder encoder) throws IOException {
    System.out.println("data store: " + encoder);
    if (null == encoder) {
      append(null);
    } else {
      append(encoder.getBytes());
    }
  }

  @Override
  protected void delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    System.out.println("delete: " + token + " " + metadata + " " + start + " " + end);
    append(("DELETE " + metadata).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  protected StandaloneDirectoryClient wrap(StandaloneDirectoryClient sdc) {
    return new WALStandaloneDirectoryClient(this, sdc);
  }

  @Override
  protected StoreClient wrap(StoreClient sdc) {
    return new WALStoreClient(this, sdc);
  }
  
  private void purge() {
    long now = System.currentTimeMillis();
    if (PURGE_DELAY > 0 && now - lastpurge > Math.min(MAXTIME, PURGE_DELAY)) {
      // Retrieve all WAL files
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
          if (null != walpath && name.equals(walpath.getName())) {
            continue;
          }
          
          WAL_MATCHER.reset(name);
          if (WAL_MATCHER.matches()) {
            long ts = Long.parseLong(WAL_MATCHER.group("ts"), 16);
            // If the file was created after the cutoff timestamp, ignore it
            if (ts >= cutoff) {
              continue;
            }
            // Delete the file
            try {
              fs.delete(status.getPath(), false);
            } catch (IOException ioe) {                    
            }
          }
        }
        lastpurge = now;
      } catch (IOException ioe) {
        LOG.error("Unable to perform WAL purge", ioe);
      }
    }
  }
  @Override
  protected void replay(StandaloneDirectoryClient sdc, StoreClient scc) {
    //
    // Purge WAL first
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
        // Only consider files which match WAL_MATCHER
        if (!WAL_MATCHER.reset(file.getPath().getName()).matches()) {
          continue;
        }
        LOG.info("Replaying " + file.getPath());
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(file.getPath()),
          SequenceFile.Reader.start(0));
        
        NullWritable key = NullWritable.get();
        BytesWritable val = new BytesWritable();
        
        long count = 0L;
        
        while(reader.next(key, val)) {
          System.out.println("VAL=" + val);
          count++;
        }
        
        LOG.info("Replayed " + count + " records.");
        reader.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error replaying WAL.", e);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
  }

  private void append(byte[] value) throws IOException {
    try {
      lock.lockInterruptibly();
      if (null == wal) {
        throw new IOException("WAL is not ready.");
      }
      if (null != value) {
        wal.append(NullWritable.get(), new BytesWritable(value));        
        wal.hsync();
        size.addAndGet(value.length);
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
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
        if (done.get() || null == wal || size.get() > MAXSIZE || System.currentTimeMillis() - start.get() > MAXTIME) {
          try {
            lock.lockInterruptibly();
            
            if (null != wal) {
              wal.hsync();
              wal.close();
              if (0 == size.get()) {
                try {
                  fs.delete(walpath, false);
                } catch (Throwable t) {                  
                }
              }
              walpath = null;
              if (done.get()) {
                break;
              }
            }
            
            long now = System.currentTimeMillis();
            String hexnow = "0000000000000000" + Long.toHexString(now);
            hexnow = hexnow.substring(hexnow.length() - 16);
            String uuid = UUID.randomUUID().toString();
            walpath = new Path(dirpath + "/" + hexnow + "." + uuid + ".wal");
            
            org.apache.hadoop.io.SequenceFile.Metadata meta = new org.apache.hadoop.io.SequenceFile.Metadata();
            meta.set(new Text("now"), new Text(String.valueOf(now)));
            meta.set(new Text("uuid"), new Text(uuid));
            wal = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(walpath),
                SequenceFile.Writer.keyClass(NullWritable.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                SequenceFile.Writer.compression(CompressionType.NONE),
                SequenceFile.Writer.metadata(meta)
                );
            size.set(0L);
            start.set(now);
          } catch (Exception e) {
            LOG.error("Error opening WAL file.", e);
            if (null != wal) {
              try { wal.close(); } catch (Throwable t) {}
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
}
