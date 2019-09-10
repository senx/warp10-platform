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
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;

/**
 * Class for decoding an encoded time serie.
 * 
 * WARNING: this class is NOT ThreadSafe as the underlying ByteBuffer could
 * be advanced by another thread while in a decoding function.
 * 
 * This is a known fact, we don't intend to make the methods synchronized so as
 * not to undermine the performances.
 */
public class CustomBufferBasedGTSDecoder extends GTSDecoder {
  
  /**
   * Buffer this decoder will decode
   */
  private CustomBuffer buffer;
  
  private TYPE lastType = null;
    
  /**
   * Base timestamp to use for this decoder
   */
  final long baseTimestamp;
  
  /**
   * Last timestamp retrieved from decoder (post call to 'next')
   */
  private long lastTimestamp = 0L;
  
  /**
   * Last location retrieved from decoder (post call to 'next')
   */
  private long lastGeoXPPoint = GeoTimeSerie.NO_LOCATION;
  
  /**
   * Last elevation retrieved from decoder (post call to 'next')
   */
  private long lastElevation = GeoTimeSerie.NO_ELEVATION;
  
  /**
   * Last long value retrieved from decoder (post call to 'next')
   */
  private long lastLongValue = Long.MAX_VALUE;
  
  /**
   * Last boolean value retrieved from decoder (post call to 'next')
   */
  private boolean lastBooleanValue = false;
  
  /**
   * Last double value retrieved from decoder (post call to 'next')
   */
  private double lastDoubleValue = Double.NaN;
  
  /**
   * Last BigDecimal value retrieved from decoder (post call to 'next')
   */
  private BigDecimal lastBDValue = null;
  
  /**
   * Last String retrieved from decoder (post call to 'next')
   */
  private String lastStringValue = null;

  private long previousLastTimestamp = lastTimestamp;
  private long previousLastGeoXPPoint = lastGeoXPPoint;
  private long previousLastElevation = lastElevation;
  private long previousLastLongValue = lastLongValue;
  private double previousLastDoubleValue = lastDoubleValue;
  private BigDecimal previousLastBDValue = lastBDValue;
  private String previousLastStringValue = lastStringValue;

  /**
   * Flag indicating whether or not 'next' was called at least once
   */
  private boolean nextCalled = false;
  
  /**
   * AES key for encrypting content
   */
  private final byte[] wrappingKey;
  
  /**
   * Metadata associated with this decoder
   */
  private Metadata metadata;
  
  /**
   * Position of the current (post call to next()) reading in the buffer
   */
  private int position;

  /**
   * Estimation of the number of elements in the decoder
   */
  private long count = 0;
  
  /**
   * @param baseTimestamp Base timestamp for computing deltas.
   * @param bb ByteBuffer containing the encoded GTS. Only remaining data will be read.
   *                      Encrypted data will silently be skipped.
   */
  public CustomBufferBasedGTSDecoder(long baseTimestamp, CustomBuffer bb) {
    super(baseTimestamp, null);
    this.baseTimestamp = baseTimestamp;
    this.buffer = bb;
    this.wrappingKey = null;
  }

  /**
   * @param baseTimestamp Base timestamp for computing deltas.
   * @param key AES Wrapping key to use for unwrapping encrypted data
   * @param bb ByteBuffer containing the encoded GTS. Only remaining data will be read.
   *                      Encrypted data that cannot be decrypted will be silently ignored.
   *                      If the buffer contains encrypted data which could be decrypted,
   *                      reallocation will take place therefore 'bb' and the internal buffer
   *                      used by this instance of GTSDecoder will
   *                      differ after the first encrypted chunk is encountered.
   */
  public CustomBufferBasedGTSDecoder(long baseTimestamp, byte[] key, CustomBuffer bb) {
    super(baseTimestamp, null);
    this.baseTimestamp = baseTimestamp;
    this.buffer = bb;
    if (null != key) {
      this.wrappingKey = Arrays.copyOfRange(key, 0, key.length);
    } else {
      this.wrappingKey = null;
    }
    this.position = bb.position();
  }

  /**
   * Attempt to read the next measurement and associated metadata (timestamp, location, elevation)
   * @return true if a measurement was successfully read, false if none were left in the buffer.
   */
  public boolean next() {
    
    //
    // Update position prior to reading the next value, etc so we can 
    //
    
    this.position = this.buffer.position();

    if (!buffer.hasRemaining()) {
      return false;
    }

    this.nextCalled = true;
    
    //
    // Read timestamp/type flag
    //
    
    byte tsTypeFlag = buffer.get();

    //
    // Check if we encountered encrypted data
    //
    
    if (GTSEncoder.FLAGS_ENCRYPTED == (tsTypeFlag & GTSEncoder.FLAGS_MASK_ENCRYPTED)) {
      //
      // Extract encrypted length
      //
      
      int enclen = (int) Varint.decodeUnsignedLong(buffer);

      //
      // If there is no decryption key, simply skip the encrypted data
      // and call next recursively.
      //
      
      if (null == wrappingKey) {
        buffer.position(buffer.position() + enclen);
        
        // WARNING(hbs): if there are many encrypted chunks this may lead to a stack overflow
        return next();
      }
      
      byte[] encrypted = new byte[enclen];
      buffer.get(encrypted);
             
      //
      // Decrypt the encrypted data
      //
      
      AESWrapEngine engine = new AESWrapEngine();
      CipherParameters params = new KeyParameter(this.wrappingKey);
      engine.init(false, params);
      
      try {
        byte[] decrypted = engine.unwrap(encrypted, 0, encrypted.length);
        //
        // Unpad the decrypted data
        //

        PKCS7Padding padding = new PKCS7Padding();
        int padcount = padding.padCount(decrypted);
        
        //
        // Replace the current buffer with a new one containing the
        // decrypted data followed by any remaining data in the original
        // buffer.
        //
        
        this.buffer.insert(decrypted, 0, decrypted.length - padcount);
      } catch (InvalidCipherTextException icte) {
        // FIXME(hbs): log this somewhere...
        //
        // Skip the encrypted chunk we failed to decrypt
        //
      }
      
      //
      // Call next recursively
      //
      // WARNING(hbs): we may hit StackOverflow in some cases
      
      return next();
    }

    //
    // Read location/elevation flag if needed
    //
    
    byte locElevFlag = 0x0;
    
    if (GTSEncoder.FLAGS_CONTINUATION == (tsTypeFlag & GTSEncoder.FLAGS_CONTINUATION)) {
      if (!buffer.hasRemaining()) {
        return false;
      }
      
      locElevFlag = buffer.get();
    }
    
    //
    // Read timestamp
    //
        
    switch (tsTypeFlag & GTSEncoder.FLAGS_MASK_TIMESTAMP) {
      case GTSEncoder.FLAGS_TIMESTAMP_RAW_ABSOLUTE: {
          ByteOrder order = buffer.order();
          buffer.order(ByteOrder.BIG_ENDIAN);
          previousLastTimestamp = lastTimestamp;
          lastTimestamp = buffer.getLong();
          buffer.order(order);
        }
        break;
      //case GTSEncoder.FLAGS_TIMESTAMP_ZIGZAG_ABSOLUTE:
      //  previousLastTimestamp = lastTimestamp;
      //  lastTimestamp = Varint.decodeSignedLong(buffer);
      //  break;
      case GTSEncoder.FLAGS_TIMESTAMP_EQUALS_BASE:
        previousLastTimestamp = lastTimestamp;
        lastTimestamp = baseTimestamp;
        break;
      case GTSEncoder.FLAGS_TIMESTAMP_ZIGZAG_DELTA_BASE: {
          long delta = Varint.decodeSignedLong(buffer);
          previousLastTimestamp = lastTimestamp;
          lastTimestamp = baseTimestamp + delta;
        }
        break;
      case GTSEncoder.FLAGS_TIMESTAMP_ZIGZAG_DELTA_PREVIOUS: {
          long delta = Varint.decodeSignedLong(buffer);
          previousLastTimestamp = lastTimestamp;
          lastTimestamp = lastTimestamp + delta;
        }
        break;
      default:
        throw new RuntimeException("Invalid timestamp format.");
    }

    //
    // Read location/elevation
    //
    
    if (GTSEncoder.FLAGS_LOCATION == (locElevFlag & GTSEncoder.FLAGS_LOCATION)) {
      if (GTSEncoder.FLAGS_LOCATION_IDENTICAL != (locElevFlag & GTSEncoder.FLAGS_LOCATION_IDENTICAL)) {
        if (GTSEncoder.FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA == (locElevFlag & GTSEncoder.FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA)) {
          long delta = Varint.decodeSignedLong(buffer);
          previousLastGeoXPPoint = lastGeoXPPoint;
          lastGeoXPPoint = lastGeoXPPoint + delta;
        } else {
          ByteOrder order = buffer.order();
          buffer.order(ByteOrder.BIG_ENDIAN);
          previousLastGeoXPPoint = lastGeoXPPoint;
          lastGeoXPPoint = buffer.getLong();
          buffer.order(order);
        } 
      }
    } else {
      previousLastGeoXPPoint = lastGeoXPPoint;
      lastGeoXPPoint = GeoTimeSerie.NO_LOCATION;
    }
    
    if (GTSEncoder.FLAGS_ELEVATION == (locElevFlag & GTSEncoder.FLAGS_ELEVATION)) {
      if (GTSEncoder.FLAGS_ELEVATION_IDENTICAL != (locElevFlag & GTSEncoder.FLAGS_ELEVATION_IDENTICAL)) {
        boolean zigzag = GTSEncoder.FLAGS_ELEVATION_ZIGZAG == (locElevFlag & GTSEncoder.FLAGS_ELEVATION_ZIGZAG);
        
        long encoded;
        
        if (zigzag) {
          encoded = Varint.decodeSignedLong(buffer);
        } else {
          ByteOrder order = buffer.order();
          buffer.order(ByteOrder.BIG_ENDIAN);
          encoded = buffer.getLong();
          buffer.order(order);          
        }
        
        if (GTSEncoder.FLAGS_ELEVATION_DELTA_PREVIOUS == (locElevFlag & GTSEncoder.FLAGS_ELEVATION_DELTA_PREVIOUS)) {
          previousLastElevation = lastElevation;
          lastElevation = lastElevation + encoded;
        } else {
          previousLastElevation = lastElevation;
          lastElevation = encoded;
        }
      }      
    } else {
      previousLastElevation = lastElevation;
      lastElevation = GeoTimeSerie.NO_ELEVATION;
    }
    
    //
    // Extract value
    //
    
    switch (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE) {
      case GTSEncoder.FLAGS_TYPE_LONG:
        lastType = TYPE.LONG;
        if (GTSEncoder.FLAGS_VALUE_IDENTICAL != (tsTypeFlag & GTSEncoder.FLAGS_VALUE_IDENTICAL)) {
          long encoded;
          
          if (GTSEncoder.FLAGS_LONG_ZIGZAG == (tsTypeFlag & GTSEncoder.FLAGS_LONG_ZIGZAG)) {
            encoded = Varint.decodeSignedLong(buffer);
          } else {
            ByteOrder order = buffer.order();
            buffer.order(ByteOrder.BIG_ENDIAN);
            encoded = buffer.getLong();
            buffer.order(order);          
          }

          if (GTSEncoder.FLAGS_LONG_DELTA_PREVIOUS == (tsTypeFlag & GTSEncoder.FLAGS_LONG_DELTA_PREVIOUS)) {
            previousLastLongValue = lastLongValue;
            lastLongValue = lastLongValue + encoded;
          } else {
            previousLastLongValue = lastLongValue;
            lastLongValue = encoded;
          }
        }
        break;
        
      case GTSEncoder.FLAGS_TYPE_DOUBLE:
        lastType = TYPE.DOUBLE;
        if (GTSEncoder.FLAGS_VALUE_IDENTICAL != (tsTypeFlag & GTSEncoder.FLAGS_VALUE_IDENTICAL)) {
          if (GTSEncoder.FLAGS_DOUBLE_IEEE754 == (tsTypeFlag & GTSEncoder.FLAGS_DOUBLE_IEEE754)) {
            ByteOrder order = buffer.order();
            buffer.order(ByteOrder.BIG_ENDIAN);
            previousLastDoubleValue = lastDoubleValue;
            lastDoubleValue = buffer.getDouble();
            previousLastBDValue = lastBDValue;
            lastBDValue = null;
            buffer.order(order);          
          } else {
            int scale = buffer.get();
            long unscaled = Varint.decodeSignedLong(buffer);
            previousLastBDValue = lastBDValue;
            lastBDValue = new BigDecimal(new BigInteger(Long.toString(unscaled)), scale);
          }
        }
        break;
        
      case GTSEncoder.FLAGS_TYPE_STRING:
        lastType = TYPE.STRING;
        if (GTSEncoder.FLAGS_VALUE_IDENTICAL != (tsTypeFlag & GTSEncoder.FLAGS_VALUE_IDENTICAL)) {
          // Decode String length
          long len = Varint.decodeUnsignedLong(buffer);
          
          // Prevent excessive allocation
          if (len > buffer.remaining()) {
            throw new RuntimeException("Invalid string length.");
          }
          
          byte[] utf8 = new byte[(int) len];
          // Read String UTF8 representation
          buffer.get(utf8);
          previousLastStringValue = lastStringValue;
          lastStringValue = new String(utf8, StandardCharsets.UTF_8);
        }
        break;
        
      case GTSEncoder.FLAGS_TYPE_BOOLEAN:
        if (GTSEncoder.FLAGS_DELETE_MARKER == (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE_FLAGS)) {
          lastType = TYPE.UNDEFINED;
        } else {
          lastType = TYPE.BOOLEAN;
          
          if (GTSEncoder.FLAGS_BOOLEAN_VALUE_TRUE == (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE_FLAGS)) {
            lastBooleanValue = true;
          } else if (GTSEncoder.FLAGS_BOOLEAN_VALUE_FALSE == (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE_FLAGS)) {
            lastBooleanValue = false;
          } else {
            throw new RuntimeException("Invalid boolean value.");
          }
          //lastBooleanValue = GTSEncoder.FLAGS_BOOLEAN_VALUE == (tsTypeFlag & GTSEncoder.FLAGS_BOOLEAN_VALUE);
        }
        break;
        
      default:
        throw new RuntimeException("Invalid type encountered!");
    }

    return true;
  }
  
  public long getTimestamp() {
    return lastTimestamp;
  }
  
  public long getLocation() {
    return lastGeoXPPoint;
  }
  
  public long getElevation() {
    return lastElevation;
  }
  
  public Object getValue() {
    switch (lastType) {
      case BOOLEAN:
        return lastBooleanValue;
      case LONG:
        return lastLongValue;
      case DOUBLE:
        return null == lastBDValue ? lastDoubleValue : lastBDValue;
      case STRING:
        return lastStringValue;
      default:
        return null;
    }
  }

  /**
   * Decode any remaining values into a GTS instance.
   * 
   * @return A GTS instance containing the remaining values.
   */
  @Override
  public GeoTimeSerie decode() {
    throw new RuntimeException("Not Implemented.");
  }
  
  
  /**
   * Return an encoder with all data from the last value retrieved (post call to next())
   * onwards
   * 
   * @param safeMetadata Is it safe to reuse the Metadata?
   */
  @Override
  public GTSEncoder getEncoder(boolean safeMetadata) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public int getRemainingSize() {
    return (int) this.buffer.remaining();
  }
  
  void initialize(long initialTimestamp, long initialGeoXPPoint, long initialElevation, long initialLongValue, double initialDoubleValue, BigDecimal initialBDValue, String initialStringValue) {
    this.lastTimestamp = initialTimestamp;
    this.lastGeoXPPoint = initialGeoXPPoint;
    this.lastElevation = initialElevation;
    this.lastLongValue = initialLongValue;
    this.lastDoubleValue = initialDoubleValue;
    this.lastBDValue = initialBDValue;
    this.lastStringValue = initialStringValue;
  } 
  
  /**
   * Returns a new instance of GTSDecoder with duplicates removed
   * 
   * WARNING: the duplicates removal is done in the order in which the values are found in the decoder. If timestamps
   * are not in chronological or reverse chronological order then you might remove values you won't be able to
   * reconstruct using FILLPREVIOUS/FILLNEXT/FILLVALUE
   * 
   * @return A GTSDecoder instance with duplicates removed
   */
  @Override
  public GTSDecoder dedup() throws IOException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public ByteBuffer getBuffer() {
    throw new RuntimeException("Not Implemented");
  }
}
