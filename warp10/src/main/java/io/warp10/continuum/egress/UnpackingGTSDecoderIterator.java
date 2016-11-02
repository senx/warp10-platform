package io.warp10.continuum.egress;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;

public class UnpackingGTSDecoderIterator extends GTSDecoderIterator {
  
  private final GTSDecoderIterator iter;
  private final String suffix;
  
  private GTSDecoder currentDecoder = null;
  
  public UnpackingGTSDecoderIterator(GTSDecoderIterator iter, String suffix) {
    this.iter = iter;
    this.suffix = suffix;
  }
  
  @Override
  public void close() throws Exception {
    iter.close();
  }
  
  @Override
  public boolean hasNext() {
    if (null != currentDecoder) {
      return true;
    }
    return iter.hasNext();
  }
  
  @Override
  public GTSDecoder next() {
    if (null == currentDecoder) {
      currentDecoder = iter.next();
    }
    
    if (!currentDecoder.getName().endsWith(suffix)) {
      throw new RuntimeException("Class does not end in '" + suffix + "'");
    }
    
    //
    // Read the next value from 'currentDecoder'
    //
    
    if (currentDecoder.next()) {
      Object value = currentDecoder.getValue();
      
      if (!(value instanceof String)) {
        throw new RuntimeException("Invalid value, expected String.");
      }
      
      //
      // Unwrap the value
      //
      
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      
      GTSWrapper wrapper = new GTSWrapper();
      try {
        deser.deserialize(wrapper, OrderPreservingBase64.decode(value.toString().getBytes(Charsets.US_ASCII)));
      } catch (TException te) {
        throw new RuntimeException(te);
      }
      
      GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
      decoder.setMetadata(currentDecoder.getMetadata());
      
      if (decoder.getMetadata().getName().endsWith(suffix)) {
        decoder.setName(decoder.getName().substring(0, decoder.getName().length() - suffix.length()));
      }
      
      if (0 == currentDecoder.getRemainingSize()) {
        currentDecoder = null;
      }
      return decoder;
    } else {
      currentDecoder = null;
      return next();
    }
  }
}
