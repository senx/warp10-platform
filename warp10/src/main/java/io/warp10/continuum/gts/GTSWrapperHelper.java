//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.continuum.gts;

import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GTSWrapperHelper {

  /**
   * Default compression ratio threshold
   */
  public static final double DEFAULT_COMP_RATIO_THRESHOLD = 100.0D;
  
  public static GTSDecoder fromGTSWrapperToGTSDecoder(GTSWrapper wrapper) {
    
    byte[] unwrapped = unwrapEncoded(wrapper);
    
    GTSDecoder decoder = new GTSDecoder(wrapper.getBase(), ByteBuffer.wrap(unwrapped).order(ByteOrder.BIG_ENDIAN));
    decoder.setMetadata(wrapper.getMetadata());
    decoder.setCount(wrapper.getCount());
    
    return decoder;
  }
  
  /**
   * convert a GTSWrapper into GeoTimeSerie
   * @param wrapper
   * @return GeoTimeSerie
   */
  public static GeoTimeSerie fromGTSWrapperToGTS(GTSWrapper wrapper) {
    return fromGTSWrapperToGTS(wrapper, false);
  }
  
  public static GeoTimeSerie fromGTSWrapperToGTS(GTSWrapper wrapper, boolean empty) {

    Metadata metadata = wrapper.getMetadata();
    GeoTimeSerie gts = null;

    if (null != wrapper.getEncoded() && !empty) {

      byte[] bytes = null;
      
      if (wrapper.isCompressed()) {
        bytes = unwrapEncoded(wrapper);
      } else {
        bytes = wrapper.getEncoded();
      }
      
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      
      GTSDecoder decoder = new GTSDecoder(wrapper.getBase(), bb);

      decoder.setCount(0 != wrapper.getCount() ? wrapper.getCount() : bytes.length / 10);

      gts = decoder.decode();

    } else {
      gts = new GeoTimeSerie();
    }

    if (null == metadata) {
      metadata = new Metadata();
    }
    
    if (wrapper.isSetBucketcount()) {
      gts.bucketcount = (int) wrapper.getBucketcount();
    }
    if (wrapper.isSetBucketspan()) {
      gts.bucketspan = wrapper.getBucketspan();
    }
    if (wrapper.isSetLastbucket()) {
      gts.lastbucket = wrapper.getLastbucket();
    }
    
    gts.setMetadata(metadata);

    return gts;
  }

  public static GTSWrapper fromGTSToGTSWrapper(GeoTimeSerie gts) {
    return fromGTSToGTSWrapper(gts, false);
  }
  
  public static GTSWrapper fromGTSEncoderToGTSWrapper(GTSEncoder encoder, boolean compress) {
    return fromGTSEncoderToGTSWrapper(encoder, compress, DEFAULT_COMP_RATIO_THRESHOLD);
  }
  
  public static GTSWrapper fromGTSEncoderToGTSWrapper(GTSEncoder encoder, boolean compress, double compratio) {
    return fromGTSEncoderToGTSWrapper(encoder, compress, compratio, Integer.MAX_VALUE);
  }
  
  public static GTSWrapper fromGTSEncoderToGTSWrapper(GTSEncoder encoder, boolean compress, double compratio, int maxpasses) {

    if (compratio < 1.0D) {
      compratio = 1.0D;
    }
        
    GTSWrapper wrapper = new GTSWrapper();

    try {      
      if (!compress || maxpasses <= 0) {
        wrapper.setEncoded(encoder.getBytes());
      } else {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] bytes = encoder.getBytes();

        double ratio = 0.0D;
        
        int pass = 0;

        //
        // We compress the data once, if the compression ratio is greater
        // than 'compratio', we consider that the resulting compressed data will
        // probably still have lots of repetitions since we will have
        // overflowed the gzip sliding window several times, we therefore
        // enter a loop to compress the data until the compression ratio
        // falls below 'compratio'
        // We then store the number of compression passes in the GTSWrapper
        // so we can apply a matching number of decompression ops
        //
        // For ultimate compression, set 'compratio' to 1.0
        //
        
        byte[] encoded = null;
        
        do {
          encoded = bytes;
          ratio = bytes.length;
          GZIPOutputStream gzos = new GZIPOutputStream(baos);
          gzos.write(bytes);
          gzos.close();
          bytes = baos.toByteArray();
          baos.reset();
          ratio = ratio / bytes.length;
          pass++;
        } while (pass < maxpasses && ratio > compratio);
        
        if (ratio > 1.0D) {
          // The last compression pass improved the footprint, so use the compressed data
          wrapper.setEncoded(bytes);
        } else {
          // The last pass added some overhead, ignore it
          pass = pass - 1;
          wrapper.setEncoded(encoded);          
        }

        if (pass > 0) {
          wrapper.setCompressed(true);
          if (pass > 1) {
            // Only store number of passes if it is > 1 as 1 is the default value
            wrapper.setCompressionPasses(pass);
          }
        } else {
          wrapper.setCompressed(false);
        }
      }
      
      wrapper.setBase(encoder.getBaseTimestamp());
      wrapper.setCount(encoder.getCount());
      wrapper.setMetadata(encoder.getMetadata());

    } catch (IOException e) {
      e.printStackTrace();
    }

    return wrapper;
  }
  
  /**
   * convert a GeoTimeSerie into GTSWrapper
   * @param gts
   * @return GTSWrapper
   */
  public static GTSWrapper fromGTSToGTSWrapper(GeoTimeSerie gts, boolean compress) {
    return fromGTSToGTSWrapper(gts, compress, DEFAULT_COMP_RATIO_THRESHOLD);
  }
  
  public static GTSWrapper fromGTSToGTSWrapper(GeoTimeSerie gts, boolean compress, double compratio) {
    return fromGTSToGTSWrapper(gts, compress, compratio, false);
  }
  
  public static GTSWrapper fromGTSToGTSWrapper(GeoTimeSerie gts, boolean compress, double compratio, boolean optimized) {

    GTSEncoder encoder = new GTSEncoder(0L);
    encoder.setMetadata(gts.getMetadata());
    
    try {
      if (optimized && TYPE.DOUBLE == gts.getType()) {
        encoder.encodeOptimized(gts);
      } else {
        encoder.encode(gts);
      }
    } catch (IOException ioe) {      
    }

    GTSWrapper wrapper = fromGTSEncoderToGTSWrapper(encoder, compress, compratio);
    
    if (GTSHelper.isBucketized(gts)) {
      wrapper.setBucketcount(gts.bucketcount);
      wrapper.setBucketspan(gts.bucketspan);
      wrapper.setLastbucket(gts.lastbucket);
    }

    return wrapper;
  }

  /**
   * Produces a GTSWrapper whose values are those at ticks from the argument only clipped to [from,to].
   * The bucketization parameters are not modified
   * 
   * @param wrapper Source wrapper
   * @return A new wrapper
   */
  public static GTSWrapper clip(GTSWrapper wrapper, long from, long to) {
    GTSDecoder decoder = new GTSDecoder(wrapper.getBase(), ByteBuffer.wrap(unwrapEncoded(wrapper)));
    GTSEncoder encoder = wrapper.isSetKey() ? new GTSEncoder(wrapper.getBase(), wrapper.getKey()) : new GTSEncoder(wrapper.getBase());
    
    while(decoder.next()) {
      if (decoder.getTimestamp() >= from && decoder.getTimestamp() <= to) {
        try {
          encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
        } catch (IOException ioe) {
          return null;
        }
      }
    }
    
    GTSWrapper clipped = new GTSWrapper();
    clipped.setBase(wrapper.getBase());
    clipped.setBucketcount(wrapper.getBucketcount());
    clipped.setBucketspan(wrapper.getBucketspan());
    clipped.setCount(encoder.getCount());
    clipped.setEncoded(encoder.getBytes());
    clipped.setLastbucket(wrapper.getLastbucket());
    clipped.setMetadata(new Metadata(wrapper.getMetadata()));
    if (wrapper.isSetKey()) {
      clipped.setKey(wrapper.getKey());
    }
    
    return clipped;
  }

  /**
   * Extract the encoded data, removing compression if needed
   * 
   * @param wrapper from which to extract the encoded data
   * @return the raw encoded data
   */
  private static byte[] unwrapEncoded(GTSWrapper wrapper) {
    
    if (!wrapper.isCompressed()) {
      return wrapper.getEncoded();
    }
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    byte[] bytes = wrapper.getEncoded();
   
    int pass = wrapper.getCompressionPasses();
    
    while(pass > 0) {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      baos.reset();
      
      try {
        GZIPInputStream gzis = new GZIPInputStream(in);
        byte[] buf = new byte[1024];
        
        while(true) {
          int len = gzis.read(buf);
          
          if (len < 0) {
            break;
          }
          
          baos.write(buf, 0, len);
        }
        
        gzis.close();          
      } catch (IOException ioe) {
        throw new RuntimeException("Invalid compressed content.");
      }
      bytes = baos.toByteArray();
      pass--;
    }

    return bytes;
  }
  
  public static boolean isBucketized(GTSWrapper gtsWrapper) {
    return 0 != gtsWrapper.getBucketcount() && 0L != gtsWrapper.getBucketspan() && 0L != gtsWrapper.getLastbucket();
  }

  /**
   * Return an ID for a wrapper
   */
  public static byte[] getId(GTSWrapper wrapper) {
    byte[] id = new byte[16];
    
    long classId = wrapper.getMetadata().getClassId();
    long labelsId = wrapper.getMetadata().getLabelsId();
    
    for (int i = 7; i >= 0; i--) {
      id[i] = (byte) (classId & 0xFFL);
      classId >>>= 8;
      
      id[8 + i] = (byte) (labelsId & 0xFFL);
      labelsId >>>= 8;
    }
    
    return id;
  }
  
  /**
   * Method used to rewrap a GTSWrapper, typically to change the compression settings.
   * 
   * @param wrapper
   * @param compress
   * @param compratio
   * @return
   */
  public static GTSWrapper rewrap(GTSWrapper wrapper, boolean compress, double compratio) {
    byte[] unwrapped = unwrapEncoded(wrapper);
    
    GTSEncoder encoder = new GTSEncoder(wrapper.getBase(), null, unwrapped);

    GTSWrapper tmp = fromGTSEncoderToGTSWrapper(encoder, compress, compratio);

    GTSWrapper rewrapped = new GTSWrapper(wrapper);
    rewrapped.setCompressed(tmp.isCompressed());
    rewrapped.setCompressionPasses(tmp.getCompressionPasses());
    rewrapped.setEncoded(tmp.getEncoded());
    
    return rewrapped;
  }
}