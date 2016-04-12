package io.warp10.hadoop;

import io.warp10.continuum.Configuration;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Warp10RecordReader extends RecordReader<Text, BytesWritable> {

  private BufferedReader br = null;
  private HttpURLConnection conn = null;

  private Text key;
  private BytesWritable value;

  private long count = 0;

  private static final Logger LOG = LoggerFactory.getLogger(Warp10RecordReader.class);

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

    int connectTimeout = Integer.valueOf(context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_HTTP_CONNECT_TIMEOUT, Warp10InputFormat.DEFAULT_WARP10_HTTP_CONNECT_TIMEOUT));

    //
    // Call each provided fetcher until one answers
    //

    String protocol = context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PROTOCOL, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PROTOCOL);
    String port = context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PORT, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PORT);
    String path = context.getConfiguration().get(Warp10InputFormat.PROPERTY_WARP10_FETCHER_PATH, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PATH);

    // FIXME: use Constants instead ?? but warp.timeunits is mandatory and property file must be provided..
    String nowHeader = context.getConfiguration().get(Configuration.HTTP_HEADER_NOW_HEADERX, Warp10InputFormat.HTTP_HEADER_NOW_HEADER_DEFAULT);
    String timespanHeader = context.getConfiguration().get(Configuration.HTTP_HEADER_TIMESPAN_HEADERX, Warp10InputFormat.HTTP_HEADER_TIMESPAN_HEADER_DEFAULT);

    for (String fetcher: split.getLocations()) {
      try {
        URL url = new URL(protocol + "://" + fetcher + ":" + port + path);

        LOG.info("Fetcher: " + url);

        conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(connectTimeout);
        conn.setChunkedStreamingMode(16384);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestProperty(nowHeader, Long.toString(now));
        conn.setRequestProperty(timespanHeader, Long.toString(timespan));
        conn.setRequestProperty("Content-Type", "application/gzip");
        conn.connect();

        OutputStream out = conn.getOutputStream();

        out.write(((Warp10InputSplit)split).getBytes());

        if (HttpURLConnection.HTTP_OK != conn.getResponseCode()) {
          System.err.println(url + " failed - error code: " + conn.getResponseCode());
          InputStream is = conn.getErrorStream();
          BufferedReader errorReader = new BufferedReader(new InputStreamReader(is));
          String line = errorReader.readLine();
          while (null != line) {
            System.err.println(line);
            line = errorReader.readLine();
          }
          is.close();
          continue;
        }

        this.br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

        break;
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
    if (null == br) {
      return false;
    }

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
