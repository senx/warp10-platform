package io.warp10.hadoop;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class Warp10RecordReader implements RecordReader<Text, BytesWritable> {

  private BufferedReader br = null;
  private HttpURLConnection conn = null;
  
  private long count = 0;
  
  public Warp10RecordReader(Warp10InputSplit split, JobConf job, Reporter progress) throws IOException {
    //
    // Call each provided fetcher until one answers
    //
    
    long now = System.currentTimeMillis() * 1000L;
    long timespan = -10;
        
    String protocol = job.get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PROTOCOL, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PROTOCOL);
    String port = job.get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PORT, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PORT);
    String path = job.get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PATH, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PATH);
    
    for (String fetcher: split.getLocations()) {
      try {
        URL url = new URL(protocol + "://" + fetcher + ":" + port + path);
        conn = (HttpURLConnection) url.openConnection();
        conn.setChunkedStreamingMode(16384);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        //conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX), Long.toString(now));
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX), Long.toString(now));
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_TIMESPAN_HEADERX), Long.toString(timespan));
        conn.setRequestProperty("Content-Type", "application/gzip");
        conn.connect();
        
        OutputStream out = conn.getOutputStream();
        
        out.write(split.getBytes());
        
        if (HttpURLConnection.HTTP_OK != conn.getResponseCode()) {
          continue;
        }
        
        this.br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (null == this.br && null != conn) {
          try { conn.disconnect(); } catch (Exception e) {}
          conn = null;
        }
      }
    }
  }

  @Override
  public boolean next(Text key, BytesWritable value) throws IOException {
    String line = br.readLine();
    
    if (null == line) {
      return false;
    }

    String[] tokens = line.split("\\s+");
    
    key.set(tokens[0]);
    
    byte[] wrapper = OrderPreservingBase64.decode(tokens[2].getBytes("US-ASCII"));

    value.setCapacity(wrapper.length);
    value.set(wrapper, 0, wrapper.length);
    
    count++;
    
    return true;
  }
  
  @Override
  public void close() throws IOException {
    this.br.close();
    this.conn.disconnect();
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public BytesWritable createValue() {
    return new BytesWritable();
  }
  
  @Override
  public long getPos() throws IOException {
    return count;
  }
  
  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
