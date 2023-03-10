//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10.continuum.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.bouncycastle.util.encoders.Hex;

import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;

import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.fdb.FDBKVScanner;
import io.warp10.fdb.FDBKeyValue;
import io.warp10.fdb.FDBPool;
import io.warp10.fdb.FDBScan;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.sensision.Sensision;

public class MultiScanGTSDecoderIterator extends GTSDecoderIterator {

  private static Random prng = new Random();

  int idx = 0;

  long nvalues = Long.MAX_VALUE;

  long toskip = 0;

  private FDBKVScanner scanner = null;

  long resultCount = 0;

  long cellCount = 0;

  private final List<Metadata> metadatas;

  private final long now;
  private final long then;

  private final ReadToken token;

  private byte[] fdbKey;

  private final long preBoundary;
  private final long postBoundary;

  private long preBoundaryCount = 0;
  private long postBoundaryCount = 0;

  // Flag indicating we scan the pre boundary.
  private boolean preBoundaryScan = false;
  // Flag indicating we scan the post boundary.
  private boolean postBoundaryScan = false;

  private long count = -1;
  private long skip = 0;
  private double sample = 1.0D;
  private long step = 1L;
  private long timestep = 1L;

  private long nextTimestamp = Long.MAX_VALUE;
  private long steps = 0L;
  private boolean hasStep = false;
  private boolean hasTimestep = false;
  private FDBPool pool;
  private byte[] tenantPrefix = null;
  private AtomicReference<Transaction> persistentTransaction = null;

  public MultiScanGTSDecoderIterator(boolean fdbUseTenantPrefix, FetchRequest req, FDBPool pool, KeyStore keystore) throws IOException {
    this.pool = pool;
    this.metadatas = req.getMetadatas();
    this.now = req.getNow();
    this.then = req.getThents();
    this.count = req.getCount();
    this.skip = req.getSkip();
    this.step = req.getStep();
    this.hasStep = step > 1L;
    this.timestep = req.getTimestep();
    this.hasTimestep = timestep > 1L;
    this.sample = req.getSample();
    this.token = req.getToken();
    this.fdbKey = keystore.getKey(KeyStore.AES_FDB_DATA);

    if (fdbUseTenantPrefix && (0 == req.getToken().getAttributesSize() || !req.getToken().getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX))) {
      throw new IOException("Invalid token, missing tenant prefix.");
    } else if (!fdbUseTenantPrefix && (0 != req.getToken().getAttributesSize() && req.getToken().getAttributes().containsKey(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX))) {
      throw new IOException("Invalid token, no support for tenant prefix.");
    }

    if (fdbUseTenantPrefix) {
      this.tenantPrefix = OrderPreservingBase64.decode(req.getToken().getAttributes().get(Constants.TOKEN_ATTR_FDB_TENANT_PREFIX));
      if (8 != this.tenantPrefix.length) {
        throw new IOException("Invalid tenant prefix, length should be 8 bytes.");
      }
    } else {
      this.tenantPrefix = pool.getContext().getTenantPrefix();
    }

    // If we are fetching up to Long.MIN_VALUE, then don't fetch a pre boundary
    this.preBoundary = Long.MIN_VALUE == then ? 0L : req.getPreBoundary();
    // If now is Long.MAX_VALUE then there is no possibility to have a post boundary
    this.postBoundary = req.getPostBoundary() >= 0L && now < Long.MAX_VALUE ? req.getPostBoundary() : 0L;

    this.postBoundaryScan = 0 != this.postBoundary;
    this.postBoundaryCount = this.postBoundary;

    this.persistentTransaction = new AtomicReference<Transaction>();
  }

  /**
   * Check whether or not there are more GeoTimeSerie instances.
   */
  @Override
  public boolean hasNext() {
    //
    // If scanner has not been nullified, it means there are more results, except if nvalues is 0
    // in which case it means we've read enough data
    //

    boolean scanHasMore = ((null == scanner) || nvalues <= 0) ? false : scanner.hasNext();

    // Adjust scanHasMore for pre/post boundaries
    if (postBoundaryScan && 0 == postBoundaryCount) {
      scanHasMore = false;
    } else if (preBoundaryScan && 0 == preBoundaryCount) {
      scanHasMore = false;
    }

    if (scanHasMore) {
      return true;
    }

    //
    // If scanner is not null close it as it does not have any more results
    //

    if (null != scanner) {
      this.scanner.close();
      this.scanner = null;

      if (postBoundaryScan) {
        // We were scanning the post boundary, the next scan will be for the core zone
        postBoundaryScan = false;
      } else if (preBoundaryScan) {
        // We were scanning the pre boundary, the next scan will be for the post boundary of the next GTS
        preBoundaryScan = false;
        postBoundaryScan = 0 != this.postBoundary;
        postBoundaryCount = this.postBoundary;
        idx++;
      } else {
        // We were scanning the core zone, now we will scan the pre boundary
        preBoundaryScan = 0 != this.preBoundary;
        preBoundaryCount = this.preBoundary;

        // If there is no pre boundary to scan, advance the metadata index
        if (!preBoundaryScan) {
          postBoundaryScan = 0 != this.postBoundary;
          postBoundaryCount = this.postBoundary;
          idx++;
        }
      }
    } else {
      // Reset the type of scan we do
      postBoundaryScan = 0 != this.postBoundary;
      preBoundaryScan = false;
      postBoundaryCount = this.postBoundary;
    }

    //
    // If there are no more metadatas then there won't be any more data
    //

    if (idx >= metadatas.size()) {
      return false;
    }

    //
    // Scanner is either exhausted or had not yet been initialized, do so now
    // Evolution of idx is performed above when closing the scanner

    Metadata metadata = metadatas.get(idx);

    //
    // Build start / end key
    //
    // CAUTION, the following code might seem wrong, but remember, timestamp
    // are reversed so the most recent (end) appears first (startkey)
    // 128bits

    byte[] startkey = new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
    // endkey has a trailing 0x00 so we include the actual end key
    byte[] endkey = new byte[startkey.length + 1];

    ByteBuffer bb = ByteBuffer.wrap(startkey).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());

    long modulus = now - (now % Constants.DEFAULT_MODULUS);

    bb.putLong(Long.MAX_VALUE - modulus);

    bb = ByteBuffer.wrap(endkey).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());

    boolean endAtMinlong = Long.MIN_VALUE == then;

    bb.putLong(Long.MAX_VALUE - then);

    //
    // Reset number of values retrieved since we just skipped to a new GTS.
    // If 'timespan' is negative this is the opposite of the number of values to retrieve
    // otherwise use Long.MAX_VALUE
    //

    nvalues = count >= 0 ? count : Long.MAX_VALUE;
    toskip = skip;
    steps = 0L;
    nextTimestamp = Long.MAX_VALUE;

    FDBScan scan = new FDBScan();

    StreamingMode mode = StreamingMode.ITERATOR;

    try {
      scan.setTenantPrefix(this.tenantPrefix);

      if (postBoundaryScan) {
        scan.setReverse(true);
        // Set the lowest (start) key to the prefix of the current start key without timestamp,
        // this is the GTS id + prefix
        scan.setStartKey(Arrays.copyOf(startkey, startkey.length - 8));
        // the end key is the start of the range
        scan.setEndKey(startkey);
      } else if (preBoundaryScan) {
        if (endAtMinlong) {
          // If the end timestamp is minlong, we will not have a preBoundaryScan, so set
          // dummy scan range
          scan.setStartKey(endkey);
          scan.setEndKey(endkey);
        } else {
          // Start right after 'endkey' (strip the last byte of endkey)
          scan.setStartKey(Arrays.copyOf(endkey, endkey.length - 1));
          byte[] k = Arrays.copyOf(endkey, endkey.length);
          // Set the reversed time stamp to 0xFFFFFFFFFFFFFFFFL plus a 0x0 byte (endkey has 1 extra byte)
          Arrays.fill(k, endkey.length - 8 - 1, k.length - 1, (byte) 0xFF);
          scan.setEndKey(k);
        }
      } else {
        scan.setStartKey(startkey);
        scan.setEndKey(endkey);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Encountered an error while setting the scan.", ioe);
    }

    if (postBoundaryScan) {
      if (postBoundary < 100) {
        mode = StreamingMode.SMALL;
      }
    } else if (preBoundaryScan) {
      if (preBoundary < 100) {
        mode = StreamingMode.SMALL;
      }
    } else {
      if (count >= 0 && count + skip < 100) {
        mode = StreamingMode.SMALL;
      }
    }

    try {
      this.scanner = scan.getScanner(pool.getContext(), pool.getDatabase(), mode, persistentTransaction);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FDB_CLIENT_SCANNERS, Sensision.EMPTY_LABELS, 1);
    } catch (IOException ioe) {
      ioe.printStackTrace();
      //
      // If we caught an exception, we skip to the next metadata
      // FIXME(hbs): log exception somehow
      //
      this.scanner = null;
      idx++;
      return hasNext();
    }

    //
    // If the current scanner has no more entries, call hasNext recursively so it takes
    // care of all the dirty details.
    //

    if (this.scanner.hasNext()) {
      return true;
    } else {
      return hasNext();
    }
  }

  @Override
  public GTSDecoder next() {
    if (null == scanner) {
      return null;
    }

    long datapoints = 0L;
    long keyBytes = 0L;
    long valueBytes = 0L;

    //
    // Create a new GTSEncoder for the results
    //

    GTSEncoder encoder = new GTSEncoder(0L);

    while(encoder.size() < Constants.MAX_ENCODER_SIZE && (nvalues > 0 || preBoundaryScan || postBoundaryScan) && scanner.hasNext()) {
      //
      // Extract next result from scanner
      //

      FDBKeyValue result = scanner.next();
      resultCount++;

      //
      // Extract timestamp base from row key
      //

      long basets = Long.MAX_VALUE;

      byte[] data = result.getKeyArray();
      int offset = result.getKeyOffset();
      offset += Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8; // Add 'prefix' + 'classId' + 'labelsId' to row key offset
      long delta = data[offset] & 0xFF;
      delta <<= 8; delta |= (data[offset + 1] & 0xFFL);
      delta <<= 8; delta |= (data[offset + 2] & 0xFFL);
      delta <<= 8; delta |= (data[offset + 3] & 0xFFL);
      delta <<= 8; delta |= (data[offset + 4] & 0xFFL);
      delta <<= 8; delta |= (data[offset + 5] & 0xFFL);
      delta <<= 8; delta |= (data[offset + 6] & 0xFFL);
      delta <<= 8; delta |= (data[offset + 7] & 0xFFL);
      basets -= delta;

      byte[] value = result.getValueArray();
      int valueOffset = result.getValueOffset();
      int valueLength = result.getValueLength();

      ByteBuffer bb = ByteBuffer.wrap(value,valueOffset,valueLength).order(ByteOrder.BIG_ENDIAN);

      GTSDecoder decoder = new GTSDecoder(basets, fdbKey, bb);

      while((nvalues > 0 || preBoundaryScan || postBoundaryScan) && decoder.next()) {
        long timestamp = decoder.getTimestamp();

        if (preBoundaryScan || postBoundaryScan || (timestamp <= now && timestamp >= then)) {
          try {
            if (!preBoundaryScan && !postBoundaryScan) {
              // Skip
              if (toskip > 0) {
                toskip--;
                continue;
              }

              // The timestamp is still after the one we expect
              if (timestamp > nextTimestamp) {
                continue;
              }

              //
              // Compute the new value of nextTimestamp if timestep is set
              //
              if (hasTimestep) {
                try {
                  nextTimestamp = Math.subtractExact(timestamp, timestep);
                } catch (ArithmeticException ae) {
                  nextTimestamp = Long.MIN_VALUE;
                  // set nvalues to 0 so we stop after the current value
                  nvalues = 0L;
                }
              }

              // We have not yet stepped over enough entries
              if (steps > 0) {
                steps--;
                continue;
              }

              if (hasStep) {
                steps = step - 1L;
              }

              // Sample if we have to
              if (1.0D != sample && prng.nextDouble() > sample) {
                continue;
              }
            }

            encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());

            //
            // Update statistics
            //

            valueBytes += valueLength;
            keyBytes += result.getKeyLength();
            datapoints++;

            // Don't decrement nvalues for the boundaries
            if (!postBoundaryScan && !preBoundaryScan) {
              nvalues--;
            }

            if (preBoundaryScan) {
              preBoundaryCount--;
              if (0 == preBoundaryCount) {
                break;
              }
            } else if (postBoundaryScan) {
              postBoundaryCount--;
              if (0 == postBoundaryCount) {
                break;
              }
            }
          } catch (IOException ioe) {
            // FIXME(hbs): LOG?
          }
        }
      }

      if (preBoundaryScan && 0 == preBoundaryCount) {
        break;
      } else if (postBoundaryScan && 0 == postBoundaryCount) {
        break;
      }
    }

    encoder.setMetadata(metadatas.get(idx));

    //
    // Update Sensision
    //

    //
    // Null token can happen when retrieving data from GTSSplit instances
    //

    if (null != token) {
      Map<String,String> labels = new HashMap<String,String>();

      Map<String,String> metadataLabels = metadatas.get(idx).getLabels();

      String billedCustomerId = Tokens.getUUID(token.getBilledId());

      if (null != billedCustomerId) {
        labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, billedCustomerId);
      }

      if (metadataLabels.containsKey(Constants.APPLICATION_LABEL)) {
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, metadataLabels.get(Constants.APPLICATION_LABEL));
      }

      if (metadataLabels.containsKey(Constants.OWNER_LABEL)) {
        labels.put(SensisionConstants.SENSISION_LABEL_OWNER, metadataLabels.get(Constants.OWNER_LABEL));
      }

      if (null != token.getAppName()) {
        labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERAPP, token.getAppName());
      }

      //
      // Update per owner statistics, use a TTL for those
      //

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, valueBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, keyBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, datapoints);

      //
      // Update summary statistics
      //

      // Remove 'owner' label
      labels.remove(SensisionConstants.SENSISION_LABEL_OWNER);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES, labels, valueBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS, labels, keyBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS, labels, datapoints);
    }

    return encoder.getDecoder();
  }

  @Override
  public void remove() {
  }

  @Override
  public void close() throws Exception {
    //
    // Update Sensision metrics
    //

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FDB_CLIENT_RESULTS, Sensision.EMPTY_LABELS, resultCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FDB_CLIENT_ITERATORS, Sensision.EMPTY_LABELS, 1);
    if (null != this.scanner) {
      this.scanner.close();
    }
    if (null != this.persistentTransaction) {
      Transaction txn = this.persistentTransaction.get();
      if (null != txn) {
        try {
          txn.close();
        } catch (Throwable t) {
        }
      }
    }
  }

}
