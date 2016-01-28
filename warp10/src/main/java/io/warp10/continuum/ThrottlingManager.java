//
//   Copyright 2016  Cityzen Data
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

package io.warp10.continuum;

import io.warp10.WarpDist;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.util.encoders.Base64;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.RateLimiter;

/**
 * This class manages the throttling of data ingestion.
 * It controls both DDP (Daily Data Points) and MADS (Monthly Active Device Streams).
 * 
 */
public class ThrottlingManager {

  public static final String LIMITS_PRODUCER_RATE_CURRENT = "producer.rate.limit";
  public static final String LIMITS_PRODUCER_MADS_LIMIT = "producer.mads.limit";
  public static final String LIMITS_PRODUCER_MADS_CURRENT = "producer.mads.current";
  
  public static final String LIMITS_APPLICATION_RATE_CURRENT = "application.rate.limit";
  public static final String LIMITS_APPLICATION_MADS_LIMIT = "application.mads.limit";
  public static final String LIMITS_APPLICATION_MADS_CURRENT = "application.mads.current";

  /**
   * Minimal limit (1 per hour) because 0 is not acceptable by RateLimiter.
   */
  public static double MINIMUM_RATE_LIMIT = 1.0D/3600.0D;
  
  /**
   * Default rate of datapoints per second per producer.
   */
  private static double DEFAULT_RATE_PRODUCER = 0.0D;
  
  /**
   * Default rate of datapoints per second per applciation.
   */
  private static double DEFAULT_RATE_APPLICATION = 0.0D;

  /**
   * Default MADS per producer
   */
  private static long DEFAULT_MADS_PRODUCER = 0L;

  /**
   * Suffix of the throttle files in the throttle dir
   */
  private static final String THROTTLING_MANAGER_SUFFIX = ".throttle";
  
  /**
   * Maximum number of HyperLogLogPlus estimators we retain in memory
   */
  private static final int ESTIMATOR_CACHE_SIZE = 10000;
  
  /**
   * Keys to compute the hash of classId/labelsId
   */
  private static final long[] SIP_KEYS = { 0x01L, 0x02L };

  /**
   * Maximum number of milliseconds to wait for RateLimiter permits
   */
  private static final long MAXWAIT_PER_DATAPOINT = 10L;
  
  /**
   * Number of milliseconds in a 30 days period
   */
  private static final long _30DAYS_SPAN = 30L * 86400L * 1000L;
  
  /**
   * Default value of 'p' parameter for estimator
   */
  private static final int DEFAULT_P = 14;
  
  /**
   * Default value of 'pprime' parameter for estimator
   */
  private static final int DEFAULT_PPRIME = 25;
  
  private static final double toleranceRatio = 1.0D + (1.04D / Math.sqrt(1L << DEFAULT_P));

  /**
   * Rate limiters to control the rate of datapoints ingestion per producer
   */
  private static Map<String,RateLimiter> producerRateLimiters = new HashMap<String, RateLimiter>();

  /**
   * Rate limiters to control the rate of datapoints ingestion per application
   */
  private static Map<String,RateLimiter> applicationRateLimiters = new HashMap<String, RateLimiter>();

  /**
   * Map of estimators for producers
   */
  private static Map<String,HyperLogLogPlus> producerHLLPEstimators = new LinkedHashMap<String, HyperLogLogPlus>() {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<String, HyperLogLogPlus> eldest) {
      //
      // Update estimator cache size
      //

      boolean overflow = this.size() > ESTIMATOR_CACHE_SIZE;
      
      if (!overflow) {
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_ESTIMATORS_CACHED, Sensision.EMPTY_LABELS, this.size());
      }
      
      return overflow;
    }
  };

  /**
   * Map of estimators for applications
   */
  private static Map<String,HyperLogLogPlus> applicationHLLPEstimators = new LinkedHashMap<String, HyperLogLogPlus>() {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<String, HyperLogLogPlus> eldest) {
      //
      // Update estimator cache size
      //

      boolean overflow = this.size() > ESTIMATOR_CACHE_SIZE;
      
      if (!overflow) {
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_ESTIMATORS_CACHED_PER_APP, Sensision.EMPTY_LABELS, this.size());
      }
      
      return overflow;
    }
  };

  private static AtomicBoolean initialized = new AtomicBoolean(false);

  private static boolean loaded = false;
  
  private static boolean enabled = false;
  
  static {
    init();
  }
  
  /**
   * Map of per producer MADS (Monthly Active Data Streams) limits
   */
  private static Map<String,Long> producerMADSLimits = new HashMap<String, Long>();

  /**
   * Map of per application MADS (Monthly Active Data Streams) limits
   */
  private static Map<String,Long> applicationMADSLimits = new HashMap<String, Long>();

  /**
   * Check compatibility of a GTS with the current MADS limit
   * 
   * @param metadata
   * @param producer
   * @param owner
   * @param application
   * @param classId
   * @param labelsId
   * @throws WarpException
   */
  public static void checkMADS(Metadata metadata, String producer, String owner, String application, long classId, long labelsId) throws WarpException {
        
    if (!loaded) {
      return;
    }
    
    //
    // Retrieve per producer limit
    //
    
    Long oProducerLimit = producerMADSLimits.get(producer);

    //
    // Extract per application limit
    //
    
    Long oApplicationLimit = applicationMADSLimits.get(application);

    // If there is no per producer limit, check the default one
    
    if (null == oProducerLimit) {      
      oProducerLimit = DEFAULT_MADS_PRODUCER;
      
      // -1 means don't check for MADS
      if (-1 == oProducerLimit && null == oApplicationLimit) {
        return;
      } else if (0 == oProducerLimit) {
        // 0 means we don't accept datastreams anymore for this producer
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
        StringBuilder sb = new StringBuilder();
        sb.append("Geo Time Series ");
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
        sb.append(" would exceed your Monthly Active Data Streams limit (");
        sb.append(oProducerLimit);
        sb.append(").");
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_GLOBAL, Sensision.EMPTY_LABELS, 1);
        throw new WarpException(sb.toString());
      }
      
      // Adjust limit so we account for the error of the estimator
      oProducerLimit = (long) Math.ceil(oProducerLimit * toleranceRatio);
    }

    long producerLimit = oProducerLimit;

    //
    // Retrieve estimator. If none is defined or if the current estimator has expired
    // was created in the previous 30 days period, allocate a new one
    //
    
    HyperLogLogPlus producerHLLP;
    
    synchronized(producerHLLPEstimators) {
      producerHLLP = producerHLLPEstimators.get(producer);
      // If the HyperLogLogPlus is older than 30 days or not yet created, generate a new one
      if (null == producerHLLP || producerHLLP.hasExpired()) {
        producerHLLP = new HyperLogLogPlus(DEFAULT_P, DEFAULT_PPRIME);
        try {
          producerHLLP.toNormal();
        } catch (IOException ioe) {
          throw new WarpException(ioe);
        }
        producerHLLPEstimators.put(producer, producerHLLP);
      }
    }
    
    //
    // Compute hash
    //
    
    long hash = GTSHelper.gtsId(SIP_KEYS, classId, labelsId);
        
    //
    // Check if hash would impact per producer cardinality, if not, return immediately if there is no per app limit
    //
    
    boolean newForProducer = producerHLLP.isNew(hash);
    
    if (!newForProducer && null == oApplicationLimit) {
      return;
    }
    
    HyperLogLogPlus applicationHLLP = null;
    
    long applicationLimit = Long.MIN_VALUE;
    
    if (null != oApplicationLimit) {
      applicationLimit = oApplicationLimit;
      
      synchronized(applicationHLLPEstimators) {
        applicationHLLP = applicationHLLPEstimators.get(application);
        // If the HyperLogLogPlus is older than 30 days or not yet created, generate a new one
        if (null == applicationHLLP || applicationHLLP.hasExpired()) {
          applicationHLLP = new HyperLogLogPlus(DEFAULT_P, DEFAULT_PPRIME);
          try {
            applicationHLLP.toNormal();
          } catch (IOException ioe) {
            throw new WarpException(ioe);
          }
          applicationHLLPEstimators.put(application, applicationHLLP);
        }
      }
    }
    
    //
    // Check per app estimator if it exists
    //
    
    if (null != applicationHLLP) {
      // If the element is not new, return immediately
      if (!applicationHLLP.isNew(hash)) {
        return;
      }
      
      try {
        long cardinality = applicationHLLP.cardinality();
        
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
        
        if (cardinality > applicationLimit) {
          StringBuilder sb = new StringBuilder();
          sb.append("Geo Time Series ");
          GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
          sb.append(" would exceed your Monthly Active Data Streams limit for application '" + application + "' (");
          sb.append((long) Math.floor(applicationLimit / toleranceRatio));
          sb.append(").");
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_PER_APP, labels, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_PER_APP_GLOBAL, Sensision.EMPTY_LABELS, 1);
          throw new WarpException(sb.toString());          
        }
        
        applicationHLLP.aggregate(hash);
        
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, applicationHLLP.cardinality());
      } catch (IOException ioe){
        // Ignore for now...
      }
    }
    
    //
    // If we are already above the monthly limit, throw an exception
    //
        
    try {
      
      long cardinality = producerHLLP.cardinality();

      Map<String,String> labels = new HashMap<String, String>();
      labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      if (cardinality > producerLimit) {
        StringBuilder sb = new StringBuilder();
        sb.append("Geo Time Series ");
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
        sb.append(" would exceed your Monthly Active Data Streams limit (");
        sb.append((long) Math.floor(producerLimit / toleranceRatio));
        sb.append(").");
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_GLOBAL, Sensision.EMPTY_LABELS, 1);
        throw new WarpException(sb.toString());
      }

      producerHLLP.aggregate(hash);
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, producerHLLP.cardinality());
    } catch (IOException ioe) {
      // Ignore for now...
    }
  }
  
  /**
   * Validate the ingestion of datapoints against the DDP limit
   * 
   * We tolerate inputs only if they wouldn't incur a wait greater than 2 seconds
   * 
   * @param producer
   * @param owner
   * @param application
   * @param count
   * @param maxwait Max wait per datapoint
   */
  public static void checkDDP(Metadata metadata, String producer, String owner, String application, int count, long maxwait) throws WarpException {
    if (!loaded) {
      return;
    }
    
    //
    // Extract RateLimiter
    //
    
    RateLimiter producerLimiter = producerRateLimiters.get(producer);
    RateLimiter applicationLimiter = applicationRateLimiters.get(application);
    
    // -1.0 as the default rate means do not enforce DDP limit
    if (null == producerLimiter && null == applicationLimiter && -1.0D == DEFAULT_RATE_PRODUCER) {      
      return;
    } else if (null == producerLimiter) {
      // Create a rate limiter with the default rate      
      producerLimiter = RateLimiter.create(Math.max(MINIMUM_RATE_LIMIT,DEFAULT_RATE_PRODUCER));
      producerRateLimiters.put(producer, producerLimiter);
    }
     
    // Check per application limiter
    if (null != applicationLimiter) {
      synchronized(applicationLimiter) {
        if (!applicationLimiter.tryAcquire(count, MAXWAIT_PER_DATAPOINT * count, TimeUnit.MILLISECONDS)) {
          StringBuilder sb = new StringBuilder();
          sb.append("Storing data for ");
          if (null != metadata) {
            GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
          }
          sb.append(" would incur a wait greater than ");
          sb.append(MAXWAIT_PER_DATAPOINT);
          sb.append(" ms per datapoint due to your Daily Data Points limit being already exceeded for application '" + application + "'. Current max rate is " + applicationLimiter.getRate() + " datapoints/s.");

          Map<String,String> labels = new HashMap<String, String>();
          labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_PER_APP, labels, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_PER_APP_GLOBAL, Sensision.EMPTY_LABELS, 1);
          
          throw new WarpException(sb.toString());      
        }
      }      
    }
    
    synchronized(producerLimiter) {
      if (!producerLimiter.tryAcquire(count, maxwait * count, TimeUnit.MILLISECONDS)) {
        StringBuilder sb = new StringBuilder();
        sb.append("Storing data for ");
        if (null != metadata) {
          GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());
        }
        sb.append(" would incur a wait greater than ");
        sb.append(MAXWAIT_PER_DATAPOINT);
        sb.append(" ms per datapoint due to your Daily Data Points limit being already exceeded. Current maximum rate is " + producerLimiter.getRate() + " datapoints/s.");

        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_GLOBAL, Sensision.EMPTY_LABELS, 1);
        
        throw new WarpException(sb.toString());      
      }
    }
  }

  public static void checkDDP(Metadata metadata, String producer, String owner, String application, int count) throws WarpException {
    checkDDP(metadata, producer, owner, application, count, MAXWAIT_PER_DATAPOINT);
  }
  
  public static Map<String,Object> getLimits(String producer, String app) {
    Map<String,Object> limits = new HashMap<String, Object>();
    
    RateLimiter producerLimiter = producerRateLimiters.get(producer);
    RateLimiter applicationLimiter = applicationRateLimiters.get(app);

    Long oProducerLimit = producerMADSLimits.get(producer);
    Long oApplicationLimit = applicationMADSLimits.get(app);

    long producerLimit = Long.MAX_VALUE;
    long applicationLimit = Long.MAX_VALUE;
    
    HyperLogLogPlus prodHLLP = producerHLLPEstimators.get(producer);
    HyperLogLogPlus appHLLP = applicationHLLPEstimators.get(app);
    
    if (null != producerLimiter) {
      limits.put(LIMITS_PRODUCER_RATE_CURRENT, producerLimiter.getRate());
    }
    
    if (null != applicationLimiter) {
      limits.put(LIMITS_APPLICATION_RATE_CURRENT, applicationLimiter.getRate());
    }
    
    if (null != oProducerLimit) {
      limits.put(LIMITS_PRODUCER_MADS_LIMIT, oProducerLimit);
      producerLimit = (long) oProducerLimit;
    }
    
    if (null != oApplicationLimit) {
      limits.put(LIMITS_APPLICATION_MADS_LIMIT, oApplicationLimit);
      applicationLimit = (long) oApplicationLimit;
    }
    
    if (null != prodHLLP) {
      try {
        long cardinality = prodHLLP.cardinality();
        
        // Change cardinality so it is capped by 'producerLimit', we don't want to expose the
        // toleranceRatio
        
        if (cardinality > producerLimit) {
          cardinality = producerLimit;
        }        
        
        limits.put(LIMITS_PRODUCER_MADS_CURRENT, cardinality);
      } catch (IOException ioe) {        
      }
    }
    
    if (null != appHLLP) {
      try {
        long cardinality = appHLLP.cardinality();
        
        // Change cardinality so it is capped by 'producerLimit', we don't want to expose the
        // toleranceRatio
        
        if (cardinality > applicationLimit) {
          cardinality = applicationLimit;
        }        
        
        limits.put(LIMITS_APPLICATION_MADS_CURRENT, cardinality);
      } catch (IOException ioe) {        
      }      
    }
    
    return limits;
  }
  
  public static void init() {    
    if (initialized.get()) {
      return;
    }
    
    final Properties properties = WarpDist.getProperties();
    
    String rate = properties.getProperty(Configuration.THROTTLING_MANAGER_RATE_DEFAULT);
    
    if (null != rate) {
      DEFAULT_RATE_PRODUCER = Double.parseDouble(rate); 
    }

    String mads = properties.getProperty(Configuration.THROTTLING_MANAGER_MADS_DEFAULT);
    
    if (null != mads) {
      DEFAULT_MADS_PRODUCER = Long.parseLong(mads);
    }
    
    //
    // Start the thread which will read the throttling configuration periodically
    //
    
    final String dir = properties.getProperty(Configuration.THROTTLING_MANAGER_DIR);

    final long now = System.currentTimeMillis();
    
    final long rampup = Long.parseLong(properties.getProperty(Configuration.THROTTLING_MANAGER_RAMPUP, "0")); 
    
    Thread t = new Thread() {
      
      // Set of files already read
      
      private Set<String> read = new HashSet<String>();
      
      long delay = Long.parseLong(properties.getProperty(Configuration.THROTTLING_MANAGER_PERIOD, "60000"));
            
      @Override
      public void run() {
        
        while(true) {
          
          //
          // If manager was not enabled, sleed then continue the loop
          //
          
          if (!enabled) {
            try { Thread.sleep(100); } catch (InterruptedException ie) {}
            continue;
          }
          
          //
          // Open the directory
          //
          
          final File root = new File(dir);
          
          String[] files = root.list(new FilenameFilter() {            
            @Override
            public boolean accept(File d, String name) {
              if (!d.equals(root)) {
                return false;
              }
              if (!name.endsWith(THROTTLING_MANAGER_SUFFIX)) {
                return false;
              }
              return true;
            }
          });
          
          // Sort files in lexicographic order
          
          if (null == files) {
            files = new String[0];
          }
          
          Arrays.sort(files);
          
          Set<String> newreads = new HashSet<String>();
                    
          for (String file: files) {
            if (read.contains(file)) {
              newreads.add(file);
              continue;
            }
            
            //
            // Read each line
            //
            
            try {
              BufferedReader br = new BufferedReader(new FileReader(new File(dir, file)));
              
              while (true) {
                String line = br.readLine();
                if (null == line) {
                  break;
                }
                line = line.trim();
                if (line.startsWith("#")) {
                  continue;
                }
                String[] tokens = line.split(":");
                
                if (5 != tokens.length) {
                  continue;
                }
                
                // Lines end with ':#'
                
                if (!"#".equals(tokens[4])) {
                  continue;
                }
                
                String entity = tokens[0];
                String mads = tokens[1];
                String rate = tokens[2];
                String estimator = tokens[3];
                
                boolean isProducer = entity.charAt(0) != '+';
                
                if (isProducer) {
                  // Attempt to read UUID
                  UUID uuid = UUID.fromString(entity);
                  entity = uuid.toString().toLowerCase();
                } else {
                  // Remove leading '+' and decode application name which may be URL encoded
                  entity = URLDecoder.decode(entity.substring(1), "UTF-8");                  
                }
                
                if ("-".equals(estimator)) {
                  //
                  // Clear estimator, we also push an event with a GTS_DISTINCT set to 0 for the producer/app
                  //
                  
                  if (isProducer) {
                    synchronized(producerHLLPEstimators) {
                      producerHLLPEstimators.remove(entity);
                      Map<String,String> labels = new HashMap<String, String>();
                      labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, entity);
                      Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, 0);              
                    }
                  } else {
                    synchronized(applicationHLLPEstimators) {
                      applicationHLLPEstimators.remove(entity);
                      Map<String,String> labels = new HashMap<String, String>();
                      labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, entity);
                      Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, 0);              
                    }
                  }
                } else if (!"".equals(estimator)) {                  
                  byte[] ser = OrderPreservingBase64.decode(estimator.getBytes(Charsets.US_ASCII));
                  HyperLogLogPlus hllp = HyperLogLogPlus.fromBytes(ser);
                  
                  // Force mode to 'NORMAL', SPARSE is too slow as it calls merge repeatdly
                  hllp.toNormal();
                  
                  //
                  // Ignore estimator if it has expired
                  //
                  
                  if (hllp.hasExpired()) {
                    hllp = new HyperLogLogPlus(hllp.getP(), hllp.getPPrime());
                    hllp.toNormal();
                    hllp.setInitTime(0);
                  }
                  
                  // Retrieve current estimator
                  
                  if (isProducer) {
                
                    isProducer = true;
                    
                    HyperLogLogPlus old = producerHLLPEstimators.get(entity);

                    // Merge estimators and replace with the result, keeping the most recent estimator as the base
                    if (null == old || hllp.getInitTime() > old.getInitTime()) {
                      if (null != old){
                        hllp.fuse(old);
                      }
                      
                      synchronized(producerHLLPEstimators) {
                        producerHLLPEstimators.put(entity, hllp);                      
                      }
                    } else {
                      old.fuse(hllp);
                    }                    
                  } else {
                    HyperLogLogPlus old = applicationHLLPEstimators.get(entity);

                    // Merge estimators and replace with the result, keeping the most recent estimator as the base
                    if (null == old || hllp.getInitTime() > old.getInitTime()) {
                      if (null != old) {
                        hllp.fuse(old);
                      }
                      
                      synchronized(applicationHLLPEstimators) {
                        applicationHLLPEstimators.put(entity, hllp);                      
                      }
                    } else {
                      old.fuse(hllp);
                    }

                  }
                }
                
                if (!"".equals(mads)) {
                  long limit = Long.parseLong(mads);
                  // Adjust limit so we account for the error of the estimator
                  limit = (long) Math.ceil(limit * toleranceRatio);
                  
                  Map<String,String> labels = new HashMap<String, String>();
                  
                  if (isProducer) {
                    producerMADSLimits.put(entity, limit);
                    labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_LIMIT, labels, limit);
                  } else {
                    applicationMADSLimits.put(entity, limit);                                        
                    labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_LIMIT_PER_APP, labels, limit);
                  }
                }
                
                if (!"".equals(rate)) {
                  Map<String,String> labels = new HashMap<String, String>();

                  double rlimit = Double.parseDouble(rate);
                  
                  if (isProducer) {
                    producerRateLimiters.put(entity, RateLimiter.create(Math.max(MINIMUM_RATE_LIMIT, rlimit)));
                    labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_LIMIT, labels, rlimit);
                  } else {
                    applicationRateLimiters.put(entity, RateLimiter.create(Math.max(MINIMUM_RATE_LIMIT, rlimit)));
                    labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_LIMIT_PER_APP, labels, rlimit);
                  }
                } else {
                  if (isProducer) {
                    producerRateLimiters.remove(entity);
                  } else {
                    applicationRateLimiters.remove(entity);
                  }
                }
                
              }
              
              br.close();
              
              newreads.add(file);
            } catch (Exception e) {              
              e.printStackTrace();
            }            
          }

          loaded = true;
          
          //
          // Replace the list of read files
          //
          
          read = newreads;
          
          //
          // Store events with the current versions of all estimators.
          //
          
          TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
              
          if (System.currentTimeMillis() - now > rampup) {
            List<String> keys = new ArrayList<String>();
            keys.addAll(producerHLLPEstimators.keySet());

            for (String key: keys) {
              HyperLogLogPlus hllp = producerHLLPEstimators.get(key);
              
              if (null == hllp) {
                continue;
              }
              
              long initTime = hllp.getInitTime();
              try {
                byte[] bytes = hllp.toBytes();
                String encoded = new String(OrderPreservingBase64.encode(bytes), Charsets.US_ASCII);
                Map<String,String> labels = new HashMap<String, String>();
                labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, key);
                Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, hllp.cardinality());              
                Sensision.event(0L, null, null, null, SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR, labels, encoded);
              } catch (IOException ioe) {
                // Ignore exception
              }
            }
            
            keys.clear();
            keys.addAll(applicationHLLPEstimators.keySet());
            
            for (String key: keys) {
              HyperLogLogPlus hllp = applicationHLLPEstimators.get(key);
              
              if (null == hllp) {
                continue;
              }
              
              long initTime = hllp.getInitTime();
              try {
                byte[] bytes = hllp.toBytes();
                String encoded = new String(OrderPreservingBase64.encode(bytes), Charsets.US_ASCII);
                Map<String,String> labels = new HashMap<String, String>();
                labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, key);
                Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, hllp.cardinality());
                Sensision.event(0L, null, null, null, SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR_PER_APP, labels, encoded);
              } catch (IOException ioe) {
                // Ignore exception
              }
            }            
          }

          try { Thread.sleep(delay); } catch (InterruptedException ie) {}
        }
      }
    };
    
    t.setName("[ThrottlingManager]");
    t.setDaemon(true);
    
    if (null != dir) {
      t.start();
    } 
    
    initialized.set(true); 
  }
  
  public static void enable() {
    enabled = true;
  }
}
