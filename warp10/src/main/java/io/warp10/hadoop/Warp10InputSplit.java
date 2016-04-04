package io.warp10.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class Warp10InputSplit extends InputSplit implements Writable {

  /**
   * Address fetchers, ideal one first
   */
  private String[] fetchers;
  
  /**
   * Byte array containing gzipped GTSSplits content
   */
  private byte[] splits;
  
  private boolean complete = false;
  
  private Set<String> fetcherSet = new LinkedHashSet<String>();
  private ByteArrayOutputStream baos = null;
  private OutputStream out = null;
  
  public Warp10InputSplit() {}    
  
  public void addEntry(String fetcher, String entry) throws IOException {
    if (this.complete) {
      throw new RuntimeException("InputSplit already completed.");
    }
    
    this.fetcherSet.add(fetcher);
  
    if (null == out) {
      baos = new ByteArrayOutputStream();
      out = new GZIPOutputStream(baos);
    }
    
    out.write(entry.getBytes("US-ASCII"));
    out.write('\r');
    out.write('\n');
  }
  
  public void addFetcher(String fetcher) {
    if (this.complete) {
      throw new RuntimeException("InputSplit already completed.");
    }
    
    this.fetcherSet.add(fetcher);
  }
  
  public Warp10InputSplit build() throws IOException {
    if (this.complete) {
      throw new RuntimeException("InputSplit already completed.");
    }

    out.close();
    this.splits = baos.toByteArray();
    baos.close();
    baos = null;
    
    this.fetchers = this.fetcherSet.toArray(new String[0]);
    
    this.complete = true;
    
    return this;
  }
  
  @Override
  public long getLength() throws IOException {
    return 0;
  }
  
  @Override
  public String[] getLocations() throws IOException {
    return Arrays.copyOf(fetchers, fetchers.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //
    // Read fetchers
    //
    
    int nfetchers = in.readInt();
    
    this.fetchers = new String[nfetchers];
    
    for (int i = 0; i < nfetchers; i++) {
      int len = in.readInt();
      byte[] buf = new byte[len];
      in.readFully(buf);
      this.fetchers[i] = new String(buf, "US-ASCII");
    }
    
    //
    // Read splits
    //
    
    int splitsize = in.readInt();
    
    this.splits = new byte[splitsize];
    
    in.readFully(this.splits);
    
    this.complete = true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    
    if (!this.complete) {
      throw new IOException("InputSplit is not completed.");      
    }
    
    out.writeInt(this.fetchers.length);
    
    for (int i = 0; i < this.fetchers.length; i++) {
      out.writeInt(this.fetchers.length);
      out.writeBytes(this.fetchers[i]);
    }
    
    out.writeInt(this.splits.length);
    out.write(this.splits);
  }
  
  public byte[] getBytes() {
    return this.splits;
  }

}
