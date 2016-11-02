package io.warp10.continuum.egress;

import java.io.IOException;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.thrift.data.Metadata;

/**
 * Wrapper for a GTSDecoderIterator which limits the size of a decoder
 * to a certain number of bytes 
 */
public class SizeLimitingGTSDecoderIterator extends GTSDecoderIterator {
  private final GTSDecoderIterator wrappedIter;
  private final int maxSize;
  
  private GTSDecoder currentDecoder = null;
  
  public SizeLimitingGTSDecoderIterator(GTSDecoderIterator iter, int maxSize) {
    this.wrappedIter = iter;
    this.maxSize = maxSize;
  }
  
  @Override
  public void close() throws Exception {
    wrappedIter.close();
  }
  
  @Override
  public boolean hasNext() {
    if (null != currentDecoder) {
      return true;
    }
    return wrappedIter.hasNext();
  }
  
  @Override
  public GTSDecoder next() {
    
    //
    // Retrieve the next decoder from the wrapped iterator
    // if we don't have one yet
    //
    
    if (null == currentDecoder) {
      currentDecoder = wrappedIter.next();
    }

    //
    // If we have a pending decoder, emit at most maxSize of it
    //
    
    // The whole decoder is less than maxSize, return it completely
    if (currentDecoder.getRemainingSize() <= maxSize) {
      GTSDecoder decoder = currentDecoder;
      this.currentDecoder = null;
      return decoder;
    }
    
    //
    // Extract at most maxSize bytes from 'currentDecoder'
    //
    
    GTSEncoder encoder = new GTSEncoder(0L);
    encoder.setMetadata(new Metadata(currentDecoder.getMetadata()));
    
    // Let's count the number of values we copied so we can
    // estimate the average size
    
    int averageValueSize = 0;
    
    while(currentDecoder.next()) {
      try {
        encoder.addValue(currentDecoder.getTimestamp(), currentDecoder.getLocation(), currentDecoder.getElevation(), currentDecoder.getValue());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      averageValueSize = (int) Math.ceil(encoder.size() / encoder.getCount());      
      
      if (maxSize - encoder.size() <= averageValueSize) {
        break;
      }
    }
    
    //
    // Geenrate a Decoder with the leftovers
    //
    
    try {
      // We call 'next()' first so we skip over the last emitted value
      currentDecoder.next();
      GTSEncoder leftovers = currentDecoder.getEncoder();

      if (leftovers.size() > 0) {
        currentDecoder = leftovers.getDecoder(true);
      } else {
        currentDecoder = null;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    
    return encoder.getDecoder(true);
  }
}
