package io.warp10.continuum.ingress;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DatalogRequest;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPOutputStream;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * Forward UPDATA/META/DELETE requests to another Warp 10 instance
 */
public class DatalogForwarder extends Thread {
  
  private static final Logger LOG = LoggerFactory.getLogger(DatalogForwarder.class);
  
  public static final String DATALOG_SUFFIX = ".datalog";
  
  /**
   * Queues to forward datalog actions according to token
   */
  private final LinkedBlockingDeque<DatalogAction>[] queues;
  
  private final Path rootdir;
  
  private final byte[] datalogPSK;
  
  /**
   * URL for the UPDATE endpoint
   */
  private final URL updateUrl;

  /**
   * URL for the DELETE endpoint
   */
  private final URL deleteUrl;
  
  /**
   * URL for the META endpoint
   */
  private final URL metaUrl;
  
  /**
   * Target directory where processed files are moved
   */
  private final File targetDir;
  
  /**
   * Period between directory scans
   */
  private final long period;
  
  /**
   * Should we compress update/meta requests we forward
   */
  private final boolean compress;
  
  /**
   * IDs we should ignore and not forward, usually to avoid loops.
   */
  private final Set<String> ignoredIds;
  
  private static final String DEFAULT_PERIOD = "1000";
    
  /**
   * Set of files currently processed
   */
  private final Set<String> processing = new HashSet<String>();

  private static enum DatalogActionType {
    UPDATE,
    DELETE,
    META
  }
  
  private static final class DatalogAction {
    private DatalogRequest request;
    private String encodedRequest;
    private File file;
  }
  
  private static final class DatalogForwarderWorker extends Thread {

    private final LinkedBlockingDeque<DatalogAction> queue;
    
    private final DatalogForwarder forwarder;
        
    public DatalogForwarderWorker(DatalogForwarder forwarder, LinkedBlockingDeque<DatalogAction> queue) {
      this.queue = queue;
      this.forwarder = forwarder;
      this.setDaemon(true);
      this.setName("[Datalog Forwarder Worker]");
      this.start();
    }
    
    @Override
    public void run() {
      
      DatalogAction action = null;
      
      while(true) {        
        action = queue.peek();
        
        if (null == action) {
          LockSupport.parkNanos(100000000L);
          continue;
        }

        boolean processed = false;
        
        try {
          switch (DatalogActionType.valueOf(action.request.getType())) {
            case DELETE:
              if (!doDelete(action)) {
                continue;
              }
              break;
            case META:
              if (!doMeta(action)) {
                continue;
              }
              break;
            case UPDATE:
              if (!doUpdate(action)) {
                continue;
              }
              break;
          }
          
          //
          // Move the file to our target directory and remove the action from the queue as we forwarded it successfully
          //
          
          if (!action.file.renameTo(new File(this.forwarder.targetDir, action.file.getName()))) {
            continue;
          }
          
          queue.poll();
          forwarder.processing.remove(action.file.getName());
          processed = true;
        } finally {
          if (!processed) {
            // Wait 10s before re-attempting the same file.
            LockSupport.parkNanos(10000000000L);
          }
        }
      }
    }
    
    private boolean doUpdate(DatalogAction action) {
      if (!DatalogActionType.UPDATE.equals(DatalogActionType.valueOf(action.request.getType()))) {
        return false;
      }
      
      BufferedReader br = null;

      HttpURLConnection conn = null;
      
      try {
        br = new BufferedReader(new FileReader(action.file));
        
        conn = (HttpURLConnection) forwarder.updateUrl.openConnection();
        
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG), action.encodedRequest);
        
        if (forwarder.compress) {
          conn.setRequestProperty("Content-Type", "application/gzip");
        }
        
        conn.setChunkedStreamingMode(16384);
        conn.connect();
        
        OutputStream os = conn.getOutputStream();
        
        OutputStream out = null;
        
        if (forwarder.compress) {
          out = new GZIPOutputStream(os);
        } else {
          out = os;
        }
        
        PrintWriter pw = new PrintWriter(out);
                
        boolean first = true;
        
        while(true) {
          String line = br.readLine();
          if (null == line) {
            break;
          }
          // Ignore first line as it is the DatalogRequest
          if (first) {
            first = false;
            continue;
          }
          pw.println(line);
        }
        
        pw.close();

        br.close();
        
        //
        // Update was successful, delete all batchfiles
        //
        
        boolean success = 200 == conn.getResponseCode();
        
        if (!success) {
          LOG.error(conn.getResponseMessage());
        }
        
        conn.disconnect();
        conn = null;

        return success;
      } catch (IOException ioe){
        ioe.printStackTrace();
        return false;
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
        if (null != br) { try { br.close(); } catch (IOException ioe) {} }
      }
    }
    
    private boolean doDelete(DatalogAction action) {
      if (!DatalogActionType.DELETE.equals(DatalogActionType.valueOf(action.request.getType()))) {
        return false;
      }
      
      BufferedReader br = null;

      HttpURLConnection conn = null;
      
      try {        
        br = new BufferedReader(new FileReader(action.file));
        
        String discardReq = br.readLine();
        String qs = br.readLine();
        
        if (null == qs) {
          return true;
        }
        
        URL urlAndQS = new URL(forwarder.deleteUrl.toString() + "?" + qs);
        
        conn = (HttpURLConnection) urlAndQS.openConnection();
        
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG), action.encodedRequest);
        conn.setChunkedStreamingMode(16384);
        conn.connect();
        
        br.close();
        
        //
        // Update was successful, delete all batchfiles
        //
        
        boolean success = 200 == conn.getResponseCode();
        
        if (!success) {
          LOG.error(conn.getResponseMessage());
        }
        
        conn.disconnect();
        conn = null;

        return success;
      } catch (IOException ioe){
        return false;
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
        if (null != br) { try { br.close(); } catch (IOException ioe) {} }
      }
    }
    
    private boolean doMeta(DatalogAction action) {
      if (!DatalogActionType.META.equals(DatalogActionType.valueOf(action.request.getType()))) {
        return false;
      }
      
      BufferedReader br = null;

      HttpURLConnection conn = null;
      
      try {
        br = new BufferedReader(new FileReader(action.file));
        
        conn = (HttpURLConnection) forwarder.metaUrl.openConnection();
        
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG), action.encodedRequest);
        
        if (forwarder.compress) {
          conn.setRequestProperty("Content-Type", "application/gzip");
        }
        
        conn.setChunkedStreamingMode(16384);
        conn.connect();
        
        OutputStream os = conn.getOutputStream();
        
        OutputStream out = null;
        
        if (forwarder.compress) {
          out = new GZIPOutputStream(os);
        } else {
          out = os;
        }
        
        PrintWriter pw = new PrintWriter(out);
                
        boolean first = true;
        
        while(true) {
          String line = br.readLine();
          if (null == line) {
            break;
          }
          // Discard the DatalogRequest line
          if (first) {
            first = false;
            continue;
          }
          pw.println(line);
        }
        
        pw.close();

        br.close();
        
        //
        // Update was successful, delete all batchfiles
        //
        
        boolean success = 200 == conn.getResponseCode();
        
        if (!success) {
          LOG.error(conn.getResponseMessage());
        }

        conn.disconnect();
        conn = null;

        return success;
      } catch (IOException ioe){
        return false;
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
        if (null != br) { try { br.close(); } catch (IOException ioe) {} }
      }
    }
    
  }
  
  public DatalogForwarder(KeyStore keystore, Properties properties) throws Exception {
    
    this.rootdir = new File(properties.getProperty(Configuration.DATALOG_DIR)).toPath();
    
    if (properties.containsKey(Configuration.DATALOG_PSK)) {
      this.datalogPSK = keystore.decodeKey(properties.getProperty(Configuration.DATALOG_PSK));
    } else {
      this.datalogPSK = null;
    }
    
    this.period = Long.parseLong(properties.getProperty(Configuration.DATALOG_FORWARDER_PERIOD, DEFAULT_PERIOD));
    
    this.compress = "true".equals(properties.getProperty(Configuration.DATALOG_FORWARDER_COMPRESS));
    
    this.ignoredIds = new HashSet<String>();
    
    if (properties.containsKey(Configuration.DATALOG_FORWARDER_IGNORED)) {
      String[] ids = properties.getProperty(Configuration.DATALOG_FORWARDER_IGNORED).split(",");
      
      for (String id: ids) {
        ignoredIds.add(id.trim());
      }
    }
    
    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_DIR)) {
      throw new RuntimeException("Datalog forwarder target directory (" +  Configuration.DATALOG_FORWARDER_DIR + ") not set.");
    }

    this.targetDir = new File(properties.getProperty(Configuration.DATALOG_FORWARDER_DIR));

    if (!this.targetDir.isDirectory()) {
      throw new RuntimeException("Invalid datalog forwarder target directory.");
    }
    
    int nthreads = Integer.parseInt(properties.getProperty(Configuration.DATALOG_FORWARDER_NTHREADS, "1"));
    
    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_ENDPOINT_UPDATE)) {
      throw new RuntimeException("Missing UPDATE endpoint.");
    }
    this.updateUrl = new URL(properties.getProperty(Configuration.DATALOG_FORWARDER_ENDPOINT_UPDATE));

    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_ENDPOINT_DELETE)) {
      throw new RuntimeException("Missing DELETE endpoint.");
    }
    this.deleteUrl = new URL(properties.getProperty(Configuration.DATALOG_FORWARDER_ENDPOINT_DELETE));

    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_ENDPOINT_META)) {
      throw new RuntimeException("Missing META endpoint.");
    }
    this.metaUrl = new URL(properties.getProperty(Configuration.DATALOG_FORWARDER_ENDPOINT_META));

    queues = new LinkedBlockingDeque[nthreads];
    
    for (int i = 0; i < nthreads; i++) {
      queues[i] = new LinkedBlockingDeque<DatalogAction>(64);
      DatalogForwarderWorker forwarder = new DatalogForwarderWorker(this, queues[i]);
    }
    
    this.setName("[Datalog Forwarder]");
    this.setDaemon(true);
    this.start();
  }
  
  @Override
  public void run() {
    while (true) {
      //
      // Scan the datalog directory
      //
      
      DirectoryStream<Path> ds = null;
      
      try {
        ds = Files.newDirectoryStream(rootdir, "*" + DATALOG_SUFFIX);        
      } catch (IOException ioe) {
        LOG.error("Error while getting file list for directory " + rootdir, ioe);
        LockSupport.parkNanos(1000000000L);
        continue;
      }
      
      Iterator<Path> iter = ds.iterator();
      
      while(iter.hasNext()) {
        //
        // Extract timestamp/id
        //
        
        Path p = iter.next();
        String filename = p.getFileName().toString();
        
        //
        // Skip file if it is currently being processed
        //
        
        if (this.processing.contains(filename)) {
          continue;
        }
        
        String[] subtokens = filename.split("-");
        
        long ts = new BigInteger(subtokens[0], 16).longValue();
        String id = subtokens[1].substring(0, subtokens[1].length() - DATALOG_SUFFIX.length());
        
        DatalogAction action = new DatalogAction();
        
        action.file = p.toFile();
        
        //
        // Read DatalogRequest
        //
        
        String encoded;
        
        try {
          BufferedReader br = new BufferedReader(new FileReader(action.file));
          encoded = br.readLine();
          br.close();          
        } catch (IOException ioe) {
          LOG.error("Error while reading Datalog Request", ioe);
          break;
        }
                
        byte[] data = OrderPreservingBase64.decode(encoded.getBytes(Charsets.US_ASCII));
        
        if (null != this.datalogPSK) {
          data = CryptoUtils.unwrap(this.datalogPSK, data);
        }
        
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        
        DatalogRequest dr = new DatalogRequest();

        try {
          deser.deserialize(dr, data);
        } catch (TException te) {
          LOG.error("Error while deserializing Datalog Request", te);
          break;
        }
        
        action.request = dr;
        action.encodedRequest = encoded;

        //
        // Check that timestamp and id match
        //
        
        if (ts != dr.getTimestamp()) {
          LOG.error("Datalog Request '" + action.file + "' has a timestamp which differs from that of its file, timestamp is 0x" + Long.toHexString(dr.getTimestamp()));
          break;
        }
        
        if (!id.equals(dr.getId())) {
          LOG.error("Datalog Request '" + action.file + "' has an id which differs from that of its file, id is " + dr.getId());
          break;          
        }
        
        //
        // Check if id should be ignored
        //
        
        String decodedId = new String(OrderPreservingBase64.decode(id.getBytes(Charsets.US_ASCII)), Charsets.UTF_8);
        if (this.ignoredIds.contains(decodedId)) {
          // File should be ignored, move it directly to the target directory
          action.file.renameTo(new File(this.targetDir, action.file.getName()));
          continue;
        }
        
        //
        // Dispatch the action to the correct queue according to the producer/app/owner of the token
        //
        
        WriteToken wtoken;
        
        try {
          wtoken = Tokens.extractWriteToken(dr.getToken());
        } catch (WarpScriptException ee) {
          LOG.error("Encountered error while extracting token.", ee);
          break;
        }

        String application = wtoken.getAppName();
        String producer = Tokens.getUUID(wtoken.getProducerId());
        String owner = Tokens.getUUID(wtoken.getOwnerId());

        String hashkey = producer + "/" + application + "/" + owner;
        
        int q = hashkey.hashCode() % queues.length;
        
        try {
          queues[q].put(action);
          processing.add(action.file.getName());
        } catch (InterruptedException ie) {
          break;
        }                                
      }
      
      try {
        ds.close();
      } catch (IOException ioe) {        
      }
      
      LockSupport.parkNanos(this.period * 1000000L);
    }
  }
}
