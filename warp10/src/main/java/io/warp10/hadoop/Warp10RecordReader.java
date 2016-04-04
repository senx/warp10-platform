package io.warp10.hadoop;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class Warp10RecordReader extends RecordReader<Text, BytesWritable> {

  private BufferedReader br = null;
  private HttpURLConnection conn = null;

  private Text key;
  private BytesWritable value;

  private long count = 0;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    if (!(split instanceof Warp10InputSplit)) {
      throw new IOException("Invalid split type.");
    }

    //
    // Retrieve now and timespan parameters
    //

    long now = Long.valueOf(context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCH_NOW));
    long timespan = Long.valueOf(context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCH_TIMESPAN));

    //
    // Call each provided fetcher until one answers
    //

    String protocol = context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PROTOCOL, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PROTOCOL);
    String port = context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PORT, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PORT);
    String path = context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PATH, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PATH);

    for (String fetcher: split.getLocations()) {
      try {
        URL url = new URL(protocol + "://" + fetcher + ":" + port + path);
        conn = (HttpURLConnection) url.openConnection();
        conn.setChunkedStreamingMode(16384);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX), Long.toString(now));
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_TIMESPAN_HEADERX), Long.toString(timespan));
        conn.setRequestProperty("Content-Type", "application/gzip");
        conn.connect();

        OutputStream out = conn.getOutputStream();

        out.write(((Warp10InputSplit)split).getBytes());

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
  public boolean nextKeyValue() throws IOException {
    String line = br.readLine();
    
    if (null == line) {
      return false;
    }

    // Format: GTSWrapperId <WSP> HASH <WSP> GTSWrapper

    String[] tokens = line.split("\\s+");

    if (null == key) {
      key = new Text();
    }

    key.set(tokens[0]);

    if (null == value) {
      value = new BytesWritable();
    }

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
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() {
    return value;
  }
  
  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
