package io.warp10;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.std.TextFileSorter;
import com.google.common.base.Charsets;

public class SortedPathIterator implements Iterator<Path> {
  
  private final Iterator<byte[]> sortedIter;
  
  public SortedPathIterator(final Iterator<Path> iter) throws IOException {
    SortConfig config = new SortConfig().withMaxMemoryUsage(1000000);
    Sorter sorter = new TextFileSorter(config);

    DataReader<byte[]> reader = new DataReader<byte[]>() {
      @Override
      public byte[] readNext() throws IOException {
        if (!iter.hasNext()) {
          return null;
        }
        return iter.next().toString().getBytes(Charsets.UTF_8);
      }
      @Override
      public void close() throws IOException {}
      @Override
      public int estimateSizeInBytes(byte[] item) { return item.length; }
    };

    this.sortedIter = sorter.sort(reader);
  }
  
  @Override
  public boolean hasNext() {
    return this.sortedIter.hasNext();
  }
  
  @Override
  public Path next() {
    File f = new File(new String(this.sortedIter.next(), Charsets.UTF_8));
    return f.toPath();
  }  
 }
