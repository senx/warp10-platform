package io.warp10.continuum;

import io.warp10.crypto.SipHashInline;

import java.util.Comparator;

import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.std.RawTextLineReader;
import com.fasterxml.sort.std.RawTextLineWriter;

public class TextFileShuffler extends Sorter<byte[]> {
  
  private static final class ShufflingComparator implements Comparator<byte[]> {
    
    /**
     * Random keys for SipHash
     */
    
    private final long k0 = System.currentTimeMillis();
    private final long k1 = System.nanoTime();
       
    @Override
    public int compare(byte[] o1, byte[] o2) {
      long h1 = SipHashInline.hash24(k0, k1, o1, 0, o1.length);
      long h2 = SipHashInline.hash24(k0, k1, o2, 0, o2.length);
      
      return Long.compare(h1, h2);
    }
  }
  
  public TextFileShuffler() {
    this(new SortConfig());
  }

  public TextFileShuffler(SortConfig config) {
    super(config, RawTextLineReader.factory(), RawTextLineWriter.factory(), new ShufflingComparator());        
  }
}
