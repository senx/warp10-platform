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

package io.warp10.standalone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import io.warp10.BytesUtils;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.leveldb.WarpDB;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;

public class StandaloneStoreClient implements StoreClient {

  /**
   * This determines how often we will perform merges when retrieving
   */
  private final long MAX_ENCODER_SIZE;

  private static final String DEFAULT_MAX_ENCODER_SIZE = "1000000";

  private final int MAX_DELETE_BATCHSIZE;
  private static final int DEFAULT_MAX_DELETE_BATCHSIZE = 10000;

  private final boolean DELETE_FILLCACHE;
  private static final boolean DEFAULT_DELETE_FILLCACHE = false;

  private final boolean DELETE_VERIFYCHECKSUMS;
  private static final boolean DEFAULT_DELETE_VERIFYCHECKSUMS = true;

  private final WarpDB db;
  private final KeyStore keystore;


  private final List<StandalonePlasmaHandlerInterface> plasmaHandlers;

  private final boolean syncwrites;
  private final double syncrate;
  private final int blockcacheThreshold;

  public StandaloneStoreClient(WarpDB db, KeyStore keystore, Properties properties) {
    this.db = db;
    this.keystore = keystore;
    this.plasmaHandlers = new ArrayList<StandalonePlasmaHandlerInterface>();
    this.blockcacheThreshold = Integer.parseInt(properties.getProperty(Configuration.LEVELDB_BLOCKCACHE_GTS_THRESHOLD, "0"));
    MAX_ENCODER_SIZE = Long.valueOf(properties.getProperty(Configuration.STANDALONE_MAX_ENCODER_SIZE, DEFAULT_MAX_ENCODER_SIZE));
    MAX_DELETE_BATCHSIZE = Integer.parseInt(properties.getProperty(Configuration.STANDALONE_MAX_DELETE_BATCHSIZE, Integer.toString(DEFAULT_MAX_DELETE_BATCHSIZE)));
    DELETE_FILLCACHE = Boolean.valueOf(properties.getProperty(Configuration.LEVELDB_DELETE_FILLCACHE, Boolean.toString(DEFAULT_DELETE_FILLCACHE)));
    DELETE_VERIFYCHECKSUMS = Boolean.valueOf(properties.getProperty(Configuration.LEVELDB_DELETE_VERIFYCHECKSUMS, Boolean.toString(DEFAULT_DELETE_VERIFYCHECKSUMS)));

    syncrate = Math.min(1.0D, Math.max(0.0D, Double.parseDouble(properties.getProperty(Configuration.LEVELDB_DATA_SYNCRATE, "1.0"))));
    syncwrites = 0.0 < syncrate && syncrate < 1.0 ;
  }

  @Override
  public GTSDecoderIterator fetch(FetchRequest req) {
    final ReadToken token = req.getToken();
    final List<Metadata> metadatas = req.getMetadatas();
    final long now = req.getNow();
    final long then = req.getThents();
    long count = req.getCount();
    long skip = req.getSkip();
    long step = req.getStep();
    long timestep = req.getTimestep();
    double sample = req.getSample();
    long preBoundary = req.getPreBoundary();
    long postBoundary = req.getPostBoundary();

    if (preBoundary < 0) {
      preBoundary = 0;
    }

    if (postBoundary < 0) {
      postBoundary = 0;
    }

    if (sample <= 0.0D || sample > 1.0D) {
      sample = 1.0D;
    }

    if (skip < 0) {
      skip = 0;
    }

    if (count < -1L) {
      count = -1L;
    }

    //
    // If we are fetching up to Long.MAX_VALUE, then don't fetch a post boundary
    if (Long.MAX_VALUE == now) {
      postBoundary = 0;
    }

    //
    // If we are fetching from Long.MIN_VALUE, then don't fetch a pre boundary
    //
    if (Long.MIN_VALUE == then) {
      preBoundary = 0;
    }

    if (step < 1L) {
      step = 1L;
    }

    if (timestep < 1L) {
      timestep = 1L;
    }

    final boolean hasStep = 1L != step;
    final boolean hasTimestep = 1L != timestep;
    final long fstep = step;
    final long ftimestep = timestep;
    // 128bits
    final byte[] rowbuf = hasTimestep ? new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8] : null;

    ReadOptions options = new ReadOptions().fillCache(true);

    if (this.blockcacheThreshold > 0) {
      if (metadatas.size() >= this.blockcacheThreshold) {
        options = new ReadOptions();
        options.fillCache(false);
      }
    }

    final DBIterator iterator = db.iterator(options);

    Map<String,String> labels = new HashMap<String,String>();

    if (null != token && null != token.getAppName()) {
      labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, token.getAppName());
    }

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_COUNT, labels, 1);

    //
    // Sort metadatas by increasing classId,labelsId so as to optimize the range scans
    //

    Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);

    final long preB = preBoundary;
    final long postB = postBoundary;

    final long fskip = skip;
    final double fsample = sample;
    final long fcount = count;

    return new GTSDecoderIterator() {

      final Random prng = fsample < 1.0D ? new Random() : null;

      //
      // The following nvalues, skip, preBoundary, postBoundary and nextTimestamp fields
      // are initialized by hasNext() when handling a new GTS.
      //

      /**
       * Number of points yet to retrieve for the current GTS.
       */
      long nvalues;

      /**
       * Number of points yet to skip because of the 'skip' parameter.
       */
      long skip;

      /**
       * Number of points before the time boundary yet to fetch.
       */
      long preBoundary;

      /**
       * Number of points after the time boundary yet to fetch.
       */
      long postBoundary;

      /**
       * Most recent timestamp to be accepted because of the 'timestep' parameter.
       */
      long nextTimestamp;

      /**
       * Number of points yet to skip because of the 'step' parameter.
       */
      long steps;

      int idx = -1;

      // First row of current scan
      byte[] startrow = null;
      // Last raw (included) of current scan
      byte[] stoprow = null;

      @Override
      public void close() throws Exception {
        iterator.close();
      }

      @Override
      public void remove() {
      }

      @Override
      public GTSDecoder next() {
        if (idx >= metadatas.size()) {
          throw new RuntimeException("Iterator is exhausted.");
        }

        GTSEncoder encoder = new GTSEncoder(0L);

        long keyBytes = 0L;
        long valueBytes = 0L;
        long datapoints = 0L;

        //
        // Fetch the boundary
        //
        while (postBoundary > 0 && encoder.size() < MAX_ENCODER_SIZE) {
          // Given the prefix for metadata is before the prefix for the raw data, the call to prev
          // will never throw an exception because we've reached the beginning of the LevelDB key space
          Entry<byte[], byte[]> kv = iterator.prev();
          // Check if the previous row is for the same GTS (prefix + 8 bytes for class id + 8 bytes for labels id)
          // 128bits
          int i = Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8;
          // The post boundary scan exhausted the datapoints for the GTS, seek back to startrow
          if (0 != BytesUtils.compareTo(kv.getKey(), 0, i, startrow, 0, i)) {
            postBoundary = 0;
            if (nvalues > 0) {
              iterator.seek(startrow);
            } else {
              // If there are no values to fetch, position at the last row
              iterator.seek(stoprow);
            }
            break;
          }

          byte[] k = kv.getKey();
          long basets = k[i++] & 0xFFL;
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets <<= 8; basets |= (k[i++] & 0xFFL);
          basets = Long.MAX_VALUE - basets;

          GTSDecoder decoder = new GTSDecoder(basets, keystore.getKey(KeyStore.AES_LEVELDB_DATA), ByteBuffer.wrap(kv.getValue()));
          decoder.next();
          try {
            encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
            postBoundary--;
            if (0 == postBoundary) {
              if (nvalues > 0) {
                iterator.seek(startrow);
              } else {
                //  If there are no values to fetch, position at the last row
                iterator.seek(stoprow);
              }
            }
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }

        //
        // Do not attempt to fetch the time range or pre boundary
        // if the postBoundary is not complete or if the encoder
        // has already reached its maximum allowed size
        //
        // We need to check iterator.hasNext otherwise the call to next() may throw an
        // exception if we've reached the end of the LevelDB key space
        if (postBoundary <= 0 && encoder.size() < MAX_ENCODER_SIZE && iterator.hasNext()) {
          do {
            Entry<byte[], byte[]> kv = iterator.next();

            // We've reached past 'stoprow', handle pre boundary
            if (BytesUtils.compareTo(kv.getKey(), stoprow) > 0) {
              //
              // If a boundary was requested, fetch it
              //

              while (preBoundary > 0 && encoder.size() < MAX_ENCODER_SIZE) {
                // Check if the row is for the same GTS, if not then we are done
                // 128bits
                int i = Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8;
                if (0 != BytesUtils.compareTo(kv.getKey(), 0, i, stoprow, 0, i)) {
                  preBoundary = 0;
                  break;
                }
                byte[] k = kv.getKey();
                long basets = k[i++] & 0xFFL;
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets <<= 8; basets |= (k[i++] & 0xFFL);
                basets = Long.MAX_VALUE - basets;

                GTSDecoder decoder = new GTSDecoder(basets, keystore.getKey(KeyStore.AES_LEVELDB_DATA), ByteBuffer.wrap(kv.getValue()));
                decoder.next();
                try {
                  encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                  preBoundary--;
                } catch (IOException ioe) {
                  throw new RuntimeException(ioe);
                }
                if (!iterator.hasNext()) {
                  break;
                }
                kv = iterator.next();
              }

              break;
            }

            // We are not fetching a pre boundary and we do not have values to fetch, exit the loop
            if (nvalues <= 0) {
              break;
            }

            ByteBuffer bb = ByteBuffer.wrap(kv.getKey()).order(ByteOrder.BIG_ENDIAN);

            bb.position(Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8);

            long basets = Long.MAX_VALUE - bb.getLong();

            byte[] v = kv.getValue();

            //
            // Skip datapoints
            //

            if (skip > 0) {
              skip--;
              continue;
            }

            //
            // Check that the datapoint timestamp is compatible with the timestep parameter, i.e. it is at least
            // 'timestep' time units before the previous one we selected
            //

            if (basets > nextTimestamp) {
              continue;
            }

            //
            // Compute the new value of nextTimestamp if timestep is set
            //
            if (hasTimestep) {
              try {
                nextTimestamp = Math.subtractExact(basets, ftimestep);
              } catch (ArithmeticException ae) {
                nextTimestamp = Long.MIN_VALUE;
                nvalues = 0L;
              }

              // TODO(hbs): should we apply a heuristics to determine if we should seek or not?

              long rowts = Long.MAX_VALUE - nextTimestamp;
              // 128bits
              int offset = Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8;
              rowbuf[offset + 7] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset + 6] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset + 5] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset + 4] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset + 3] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset + 2] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset + 1] = (byte) (rowts & 0xFFL);
              rowts >>>= 8;
              rowbuf[offset] = (byte) (rowts & 0xFFL);

              iterator.seek(rowbuf);
            }

            //
            // Check that the data point should not be stepped over
            //

            if (steps > 0) {
              steps--;
              continue;
            }

            if (hasStep) {
              steps = fstep - 1L;
            }

            //
            // Sample datapoints
            //

            if (fsample < 1.0D && prng.nextDouble() > fsample) {
              continue;
            }

            valueBytes += v.length;
            keyBytes += kv.getKey().length;
            datapoints++;

            nvalues--;

            GTSDecoder decoder = new GTSDecoder(basets, keystore.getKey(KeyStore.AES_LEVELDB_DATA), ByteBuffer.wrap(kv.getValue()));
            decoder.next();
            try {
              encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          } while(iterator.hasNext() && encoder.size() < MAX_ENCODER_SIZE && nvalues > 0);
        }

        encoder.setMetadata(metadatas.get(idx));

        //
        // Update Sensision
        //

        Map<String,String> labels = new HashMap<String,String>();

        Map<String,String> metadataLabels = metadatas.get(idx).getLabels();

        String billedCustomerId = null != token ? Tokens.getUUID(token.getBilledId()) : null;

        if (null != billedCustomerId) {
          labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, billedCustomerId);
        }

        if (metadataLabels.containsKey(Constants.APPLICATION_LABEL)) {
          labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, metadataLabels.get(Constants.APPLICATION_LABEL));
        }

        if (metadataLabels.containsKey(Constants.OWNER_LABEL)) {
          labels.put(SensisionConstants.SENSISION_LABEL_OWNER, metadataLabels.get(Constants.OWNER_LABEL));
        }

        if (null != token && null != token.getAppName()) {
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

        return encoder.getDecoder();
      }

      @Override
      public boolean hasNext() {
        // All the metadatas have been itered on, there is no more GTSEncoder to return.
        if (idx >= metadatas.size()) {
          return false;
        }

        // While all the metadata are exhausted or there is potentially some data associated to a metadata.
        while(true) {
          // Check if there are still some data associated with the current metadata.
          if (idx >= 0) {
            // Still potential data if iterator has a previous value and postBoundary is strictly positive.
            // The definitive check to whether there is data or not is in next(). If no data is found,
            // postBoundary will be set to 0 and the next call to hasNext will fail on this test.
            if (postBoundary > 0 && iterator.hasPrev()) {
              return true;
            }

            if (iterator.hasNext()) {
              // Still potential data if iterator has a next value and preBoundary is strictly positive.
              // The definitive check to whether there is data or not is in next(). If no data is found,
              // preBoundary will be set to 0 and the next call to hasNext will fail on this test.
              if(preBoundary > 0) {
                return true;
              }

              // Still some data if fetch is either time based or has not returned the requested number of points and stoprow was not yet reached.
              byte[] key = iterator.peekNext().getKey();
              if ((-1 == fcount || nvalues > 0) && (BytesUtils.compareTo(key, stoprow) <= 0)) {
                return true;
              }
            }
          }

          // No more data associated with the current metadata, go to the next metadata.
          idx++;

          // All the metadatas have been itered on, there is no more GTSEncoder to return.
          if (idx >= metadatas.size()) {
            return false;
          }

          // 128bits
          startrow = new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
          ByteBuffer bb = ByteBuffer.wrap(startrow).order(ByteOrder.BIG_ENDIAN);
          bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
          bb.putLong(metadatas.get(idx).getClassId());
          bb.putLong(metadatas.get(idx).getLabelsId());
          bb.putLong(Long.MAX_VALUE - now);

          stoprow = new byte[startrow.length];
          bb = ByteBuffer.wrap(stoprow).order(ByteOrder.BIG_ENDIAN);
          bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
          bb.putLong(metadatas.get(idx).getClassId());
          bb.putLong(metadatas.get(idx).getLabelsId());
          bb.putLong(Long.MAX_VALUE - then);

          if (null != rowbuf) {
            System.arraycopy(startrow, 0, rowbuf, 0, rowbuf.length);
          }

          //
          // Reset number of values retrieved since we just skipped to a new GTS.
          // If 'timespan' is negative this is the opposite of the number of values to retrieve
          // otherwise use Long.MAX_VALUE
          //

          nvalues = fcount >= 0L ? fcount : Long.MAX_VALUE;

          skip = fskip;
          preBoundary = preB;
          postBoundary = postB;
          nextTimestamp = Long.MAX_VALUE;
          steps = 0L;

          // If we are not fetching a post boundary and not fetching data from the
          // defined time range, seek to stoprow to speed up possible pre boundary
          // fetch
          if (0 == nvalues && postBoundary <= 0) {
            iterator.seek(stoprow);
          } else {
            iterator.seek(startrow);
          }
        }
      }
    };
  }

  private ThreadLocal<WriteBatch> perThreadWriteBatch = new ThreadLocal<WriteBatch>() {
    protected WriteBatch initialValue() {
      return db.createWriteBatch();
    };
  };

  private ThreadLocal<AtomicLong> perThreadWriteBatchSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong(0L);
    };
  };

  private void store(List<byte[][]> kvs) throws IOException {

    WriteBatch batch = perThreadWriteBatch.get();

    AtomicLong size = perThreadWriteBatchSize.get();

    boolean written = false;

    try {
      if (null != kvs) {
        for (byte[][] kv: kvs) {
          batch.put(kv[0], kv[1]);
          size.addAndGet(kv[0].length + kv[1].length);
        }
      }

      if (null == kvs || size.get() > MAX_ENCODER_SIZE) {

        WriteOptions options = new WriteOptions().sync(null == kvs || 1.0 == syncrate);

        if (syncwrites && !options.sync()) {
          options = new WriteOptions().sync(Math.random() < syncrate);
        }

        this.db.write(batch, options);
        size.set(0L);
        perThreadWriteBatch.remove();
        written = true;
      }
      //this.db.write(batch);
    } finally {
      if (written) {
        batch.close();
      }
    }
  }

  public void store(GTSEncoder encoder) throws IOException {

    if (null == encoder) {
      store((List<byte[][]>) null);
      return;
    }

    GTSDecoder decoder = encoder.getDecoder();

    List<byte[][]> kvs = new ArrayList<byte[][]>();

    while(decoder.next()) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8]).order(ByteOrder.BIG_ENDIAN);
      bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
      bb.putLong(encoder.getClassId());
      bb.putLong(encoder.getLabelsId());
      bb.putLong(Long.MAX_VALUE - decoder.getTimestamp());

      GTSEncoder enc = new GTSEncoder(decoder.getTimestamp(), this.keystore.getKey(KeyStore.AES_LEVELDB_DATA));

      enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());

      byte[] value = enc.getBytes();

      kvs.add(new byte[][] { bb.array(), value });
    }

    store(kvs);

    for (StandalonePlasmaHandlerInterface plasmaHandler: this.plasmaHandlers) {
      if (plasmaHandler.hasSubscriptions()) {
        plasmaHandler.publish(encoder);
      }
    }
  }

  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {

    if (null == metadata) {
      return 0L;
    }

    //
    // Regen classId/labelsId
    //

    // 128BITS
    metadata.setLabelsId(GTSHelper.labelsId(this.keystore.getKey(KeyStore.SIPHASH_LABELS), metadata.getLabels()));
    metadata.setClassId(GTSHelper.classId(this.keystore.getKey(KeyStore.SIPHASH_CLASS), metadata.getName()));

    //
    // Retrieve an iterator
    //

    DBIterator iterator = null;

    WriteBatch batch = null;

    try {
      ReadOptions roptions = new ReadOptions();
      roptions.fillCache(DELETE_FILLCACHE);
      roptions.verifyChecksums(DELETE_VERIFYCHECKSUMS);

      iterator = this.db.iterator(roptions);

      //
      // Seek the most recent key
      //

      // 128BITS
      byte[] bend = new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
      ByteBuffer bb = ByteBuffer.wrap(bend).order(ByteOrder.BIG_ENDIAN);
      bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
      bb.putLong(metadata.getClassId());
      bb.putLong(metadata.getLabelsId());
      bb.putLong(Long.MAX_VALUE - end);

      iterator.seek(bend);

      byte[] bstart = new byte[bend.length];
      bb = ByteBuffer.wrap(bstart).order(ByteOrder.BIG_ENDIAN);
      bb.put(Constants.FDB_RAW_DATA_KEY_PREFIX);
      bb.putLong(metadata.getClassId());
      bb.putLong(metadata.getLabelsId());
      bb.putLong(Long.MAX_VALUE - start);

      //
      // Scan the iterator, deleting keys if they are between start and end
      //

      long count = 0L;

      batch = this.db.createWriteBatchUnlocked();
      int batchsize = 0;

      WriteOptions options = new WriteOptions().sync(1.0 == syncrate);

      while (iterator.hasNext()) {
        Entry<byte[],byte[]> entry = iterator.next();

        if (BytesUtils.compareTo(entry.getKey(), bend) >= 0 && BytesUtils.compareTo(entry.getKey(), bstart) <= 0) {
          batch.delete(entry.getKey());
          batchsize++;

          if (MAX_DELETE_BATCHSIZE <= batchsize) {
            if (syncwrites) {
              options = new WriteOptions().sync(Math.random() < syncrate);
            }
            this.db.writeUnlocked(batch, options);
            batch.close();
            batch = this.db.createWriteBatchUnlocked();
            batchsize = 0;
          }
          //this.db.delete(entry.getKey());
          count++;
        } else {
          break;
        }
      }

      if (batchsize > 0) {
        if (syncwrites) {
          options = new WriteOptions().sync(Math.random() < syncrate);
        }
        this.db.writeUnlocked(batch, options);
      }
      return count;
    } finally {
      //
      // We need to close those so pendingOps is correctly updated
      //
      if (null != iterator) {
        try {
          iterator.close();
        } catch (Throwable t) {
        }
      }
      if (null != batch) {
        try {
          batch.close();
        } catch (Throwable t) {
        }
      }
    }
  }

  public void addPlasmaHandler(StandalonePlasmaHandlerInterface plasmaHandler) {
    this.plasmaHandlers.add(plasmaHandler);
  }
}
