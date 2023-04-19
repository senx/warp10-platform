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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.WarpException;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.fdb.FDBUtils;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.DURATION;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.IngressPlugin;

public class StandaloneIngressHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneIngressHandler.class);

  /**
   * How many bytes do we buffer before flushing to persistent storage, this will also end up being the size of each archival chunk
   */
  public static final int ENCODER_SIZE_THRESHOLD = 500000;

  /**
   * How often (in number of ingested datapoints) to check for pushback
   */
  public static final long PUSHBACK_CHECK_INTERVAL = 1000L;

  /**
   * Default max size of value
   */
  public static final String DEFAULT_VALUE_MAXSIZE = "65536";

  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final StandaloneDirectoryClient directoryClient;

  private final byte[] classKey;
  private final byte[] labelsKey;

  private final long[] classKeyLongs;
  private final long ckl0;
  private final long ckl1;
  private final long[] labelsKeyLongs;
  private final long lkl0;
  private final long lkl1;
  private final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSS").withZoneUTC();

  private final long maxValueSize;

  private final boolean updateActivity;
  private final boolean metaActivity;
  private final boolean parseAttributes;
  private final Long maxpastDefault;
  private final Long maxfutureDefault;
  private final Long maxpastOverride;
  private final Long maxfutureOverride;
  private final boolean ignoreOutOfRange;
  private final boolean allowDeltaAttributes;

  private final IngressPlugin plugin;
  
  private final boolean isFDBStore; // skip FDB tests when not necessary

  public StandaloneIngressHandler(KeyStore keystore, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {

    this.keyStore = keystore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;

    this.classKey = this.keyStore.getKey(KeyStore.SIPHASH_CLASS);
    this.classKeyLongs = SipHashInline.getKey(this.classKey);
    this.ckl0 = this.classKeyLongs[0];
    this.ckl1 = this.classKeyLongs[1];

    this.labelsKey = this.keyStore.getKey(KeyStore.SIPHASH_LABELS);
    this.labelsKeyLongs = SipHashInline.getKey(this.labelsKey);
    this.lkl0 = this.labelsKeyLongs[0];
    this.lkl1 = this.labelsKeyLongs[1];

    this.allowDeltaAttributes = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_ATTRIBUTES_ALLOWDELTA));

    updateActivity = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_ACTIVITY_UPDATE));
    metaActivity = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_ACTIVITY_META));

    this.parseAttributes = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_PARSE_ATTRIBUTES));
    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT)) {
      maxpastDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT));
      if (maxpastDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_DEFAULT + "' MUST be positive.");
      }
    } else {
      maxpastDefault = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT)) {
      maxfutureDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT));
      if (maxfutureDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_DEFAULT + "' MUST be positive.");
      }
    } else {
      maxfutureDefault = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE)) {
      maxpastOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE));
      if (maxpastOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_OVERRIDE + "' MUST be positive.");
      }
    } else {
      maxpastOverride = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE)) {
      maxfutureOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE));
      if (maxfutureOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_OVERRIDE + "' MUST be positive.");
      }
    } else {
      maxfutureOverride = null;
    }

    this.ignoreOutOfRange = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_OUTOFRANGE_IGNORE));
    if (null != WarpConfig.getProperty(Configuration.INGRESS_PLUGIN_CLASS)) {
      try {
        ClassLoader pluginCL = this.getClass().getClassLoader();

        Class pluginClass = Class.forName(WarpConfig.getProperty(Configuration.INGRESS_PLUGIN_CLASS), true, pluginCL);
        this.plugin = (IngressPlugin) pluginClass.newInstance();

        //
        // Now call the 'init' method of the plugin
        //

        this.plugin.init();
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate plugin class", e);
      }
    } else {
      this.plugin = null;
    }

    this.maxValueSize = Long.parseLong(WarpConfig.getProperty(Configuration.STANDALONE_VALUE_MAXSIZE, DEFAULT_VALUE_MAXSIZE));
    
    this.isFDBStore = Constants.BACKEND_FDB.equals(WarpConfig.getProperty(Configuration.BACKEND));
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

    String token = null;

    if (target.equals(Constants.API_ENDPOINT_UPDATE)) {
      baseRequest.setHandled(true);
    } else if (target.startsWith(Constants.API_ENDPOINT_UPDATE + "/")) {
      baseRequest.setHandled(true);
      token = target.substring(Constants.API_ENDPOINT_UPDATE.length() + 1);
    } else if (target.equals(Constants.API_ENDPOINT_META)) {
      handleMeta(target, baseRequest, request, response);
      return;
    } else {
      return;
    }

    if (null != WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)));
      return;
    }

    long lastActivity = System.currentTimeMillis();
    int httpStatusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

    try {
      WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_SESSION, UUID.randomUUID().toString());

      //
      // CORS header
      //

      response.setHeader("Access-Control-Allow-Origin", "*");

      long nano = System.nanoTime();

      boolean deltaAttributes = "delta".equals(request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES)));

      if (deltaAttributes && !this.allowDeltaAttributes) {
        httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
        throw new IOException("Delta update of attributes is disabled.");
      }
      boolean nocache = AcceleratorConfig.getDefaultWriteNocache();
      // boolean to indicate we were explicitely instructed a nocache value
      boolean forcedNocache = false;
      boolean nopersist = AcceleratorConfig.getDefaultWriteNopersist();
      // boolean to indicate we were explicitely instructed a nopersist value
      boolean forcedNopersist = false;

      if (null != request.getHeader(AcceleratorConfig.ACCELERATOR_HEADER)) {
        if (request.getHeader(AcceleratorConfig.ACCELERATOR_HEADER).contains(AcceleratorConfig.NOCACHE)) {
          nocache = true;
          forcedNocache = true;
        } else if (request.getHeader(AcceleratorConfig.ACCELERATOR_HEADER).contains(AcceleratorConfig.CACHE)) {
          nocache = false;
          forcedNocache = true;
        }
        if (request.getHeader(AcceleratorConfig.ACCELERATOR_HEADER).contains(AcceleratorConfig.NOPERSIST)) {
          nopersist = true;
          forcedNopersist = true;
        } else if (request.getHeader(AcceleratorConfig.ACCELERATOR_HEADER).contains(AcceleratorConfig.PERSIST)) {
          nopersist = false;
          forcedNopersist = true;
        }
      }

      if (nocache) {
        AcceleratorConfig.nocache();
      } else {
        AcceleratorConfig.cache();
      }

      if (nopersist) {
        AcceleratorConfig.nopersist();
      } else {
        AcceleratorConfig.persist();
      }

      //
      // TODO(hbs): Extract producer/owner from token
      //

      if (null == token) {
        token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
      }

      WriteToken writeToken;

      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOUPDATE)) {
          httpStatusCode = HttpServletResponse.SC_FORBIDDEN;
          throw new WarpScriptException("Token cannot be used for updating data.");
        }
      } catch (WarpScriptException ee) {
        httpStatusCode = HttpServletResponse.SC_FORBIDDEN;
        throw ee;
      }

      WarpConfig.setThreadProperty(WarpConfig.THREAD_PROPERTY_TOKEN, writeToken);

      long maxsize = maxValueSize;

      if (writeToken.getAttributesSize() > 0 && null != writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE)) {
        maxsize = Long.parseLong(writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE));
      }

      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());

      Map<String,String> sensisionLabels = new HashMap<String,String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      long count = 0;
      long total = 0;

      File loggingFile = null;
      PrintWriter loggingWriter = null;

      long shardkey = 0L;
      FileDescriptor loggingFD = null;

      boolean hasDatapoints = false;

      boolean expose = false;

      try {
        if (null == producer || null == owner) {
          response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
          return;
        }

        //
        // Build extra labels
        //

        Map<String,String> extraLabels = new HashMap<String,String>();

        // Add labels from the WriteToken if they exist
        if (writeToken.getLabelsSize() > 0) {
          extraLabels.putAll(writeToken.getLabels());
        }

        // Force internal labels
        extraLabels.put(Constants.PRODUCER_LABEL, producer);
        extraLabels.put(Constants.OWNER_LABEL, owner);
        // FIXME(hbs): remove me, apps should be set in all tokens now...
        if (null != application) {
          extraLabels.put(Constants.APPLICATION_LABEL, application);
          sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
        } else {
          // remove application label
          extraLabels.remove(Constants.APPLICATION_LABEL);
        }

        //
        // Determine if content if gzipped
        //

        boolean gzipped = false;

        if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {
          gzipped = true;
        }

        BufferedReader br = null;

        if (gzipped) {
          GZIPInputStream is = new GZIPInputStream(request.getInputStream());
          br = new BufferedReader(new InputStreamReader(is));
        } else {
          br = request.getReader();
        }

        //
        // Get the present time
        //

        Long now = TimeSource.getTime();

        //
        // Extract time limits
        //

        Long maxpast = null;

        if (null != maxpastDefault) {
          try {
            maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxpastDefault));
          } catch (ArithmeticException ae) {
            maxpast = null;
          }
        }

        Long maxfuture = null;

        if (null != maxfutureDefault) {
          try {
            maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxfutureDefault));
          } catch (ArithmeticException ae) {
            maxfuture = null;
          }
        }

        Boolean ignoor = null;

        if (writeToken.getAttributesSize() > 0) {

          expose = writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);

          if (writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_IGNOOR)) {
            String v = writeToken.getAttributes().get(Constants.TOKEN_ATTR_IGNOOR).toLowerCase();
            if ("true".equals(v) || "t".equals(v)) {
              ignoor = Boolean.TRUE;
            } else if ("false".equals(v) || "f".equals(v)) {
              ignoor = Boolean.FALSE;
            }
          }

          String deltastr = writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXPAST);

          if (null != deltastr) {
            long delta = Long.parseLong(deltastr);
            if (delta < 0) {
              httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
              throw new WarpScriptException("Invalid '" + Constants.TOKEN_ATTR_MAXPAST + "' token attribute, MUST be positive.");
            }
            try {
              maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, delta));
            } catch (ArithmeticException ae) {
              maxpast = null;
            }
          }

          deltastr = writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXFUTURE);

          if (null != deltastr) {
            long delta = Long.parseLong(deltastr);
            if (delta < 0) {
              httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
              throw new WarpScriptException("Invalid '" + Constants.TOKEN_ATTR_MAXFUTURE + "' token attribute, MUST be positive.");
            }
            try {
              maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, delta));
            } catch (ArithmeticException ae) {
              maxfuture = null;
            }
          }
        }

        if (null != maxpastOverride) {
          try {
            maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxpastOverride));
          } catch (ArithmeticException ae) {
            maxpast = null;
          }
        }

        if (null != maxfutureOverride) {
          try {
            maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxfutureOverride));
          } catch (ArithmeticException ae) {
            maxfuture = null;
          }
        }

        //
        // Check the value of the 'now' header
        //
        // The following values are supported:
        //
        // A number, which will be interpreted as an absolute time reference,
        // i.e. a number of time units since the Epoch.
        //
        // A number prefixed by '+' or '-' which will be interpreted as a
        // delta from the present time.
        //
        // A '*' which will mean to not set 'now', and to recompute its value
        // each time it's needed.
        //

        String nowstr = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX));

        if (null != nowstr) {
          if ("*".equals(nowstr)) {
            now = null;
          } else if (nowstr.startsWith("+")) {
            try {
              long delta = Long.parseLong(nowstr.substring(1));
              now = now + delta;
            } catch (Exception e) {
              httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
              throw new IOException("Invalid base timestamp.");
            }
          } else if (nowstr.startsWith("-")) {
            try {
              long delta = Long.parseLong(nowstr.substring(1));
              now = now - delta;
            } catch (Exception e) {
              httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
              throw new IOException("Invalid base timestamp.");
            }
          } else {
            try {
              now = Long.parseLong(nowstr);
            } catch (Exception e) {
              httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
              throw new IOException("Invalid base timestamp.");
            }
          }
        }


        //
        // Check the value of the timeshift header
        //

        String timeshiftstr = request.getHeader(Constants.HTTP_HEADER_TIMESHIFT);

        long timeshift = 0L;

        if (null != timeshiftstr) {
          if (timeshiftstr.startsWith("P")) {
            timeshift = DURATION.parseDuration(new Instant(), timeshiftstr, false, false);
          } else {
            try {
              timeshift = Long.parseLong(timeshiftstr);
            } catch (NumberFormatException nfe) {
              httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
              throw new IOException("Invalid timeshift.");
            }
          }
        }

        //
        // Loop on all lines
        //

        GTSEncoder lastencoder = null;
        GTSEncoder encoder = null;

        // Atomic boolean to track if attributes were parsed
        AtomicBoolean hadAttributes = parseAttributes ? new AtomicBoolean(false) : null;

        //
        // Chunk index when archiving
        //

        boolean lastHadAttributes = false;

        AtomicLong ignoredCount = null;

        if ((ignoreOutOfRange && !Boolean.FALSE.equals(ignoor)) || Boolean.TRUE.equals(ignoor)) {
          ignoredCount = new AtomicLong(0L);
        }

        do {

          if (parseAttributes) {
            lastHadAttributes = lastHadAttributes || hadAttributes.get();
            hadAttributes.set(false);
          }

          String line = br.readLine();

          if (null == line) {
            break;
          }

          line = line.trim();

          if (0 == line.length()) {
            continue;
          }

          //
          // Ignore comments
          //

          if ('#' == line.charAt(0)) {
            continue;
          }

          //
          // Check for pushback
          // TODO(hbs): implement the actual push back if we are over the subscribed limit
          //

          if (count % PUSHBACK_CHECK_INTERVAL == 0) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
            total += count;
            count = 0;
          }

          count++;

          try {
            encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, maxsize, hadAttributes, maxpast, maxfuture, ignoredCount, deltaAttributes, timeshift);
            if (null != this.plugin) {
              if (!this.plugin.update(this, writeToken, line, encoder)) {
                hadAttributes.set(false);
                continue;
              }
            }
            //nano2 += System.nanoTime() - nano0;
          } catch (ParseException pe) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_PARSEERRORS, sensisionLabels, 1);
            httpStatusCode = HttpServletResponse.SC_BAD_REQUEST;
            throw new IOException("Parse error at index " + pe.getErrorOffset() + " in '" + line + "'", pe);
          }

          if (encoder != lastencoder || lastencoder.size() > ENCODER_SIZE_THRESHOLD || (isFDBStore && FDBUtils.hasCriticalTransactionSize(lastencoder, maxValueSize))) {

            //
            // Check throttling
            //

            if (null != lastencoder) {

              // 128BITS
              lastencoder.setClassId(GTSHelper.classId(ckl0, ckl1, lastencoder.getName()));
              lastencoder.setLabelsId(GTSHelper.labelsId(lkl0, lkl1, lastencoder.getMetadata().getLabels()));

              try {
                ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId(), expose);
                ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount(), expose);
              } catch (WarpException we) {
                httpStatusCode = 429; // Too Many Requests
                throw we;
              }
            }

            //
            // Build metadata object to push
            //

            if (encoder != lastencoder) {
              Metadata metadata = new Metadata(encoder.getMetadata());
              metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
              if (this.updateActivity) {
                metadata.setLastActivity(lastActivity);
              }

              this.directoryClient.register(metadata);

              // Extract shardkey 128BITS
              // Shard key is 48 bits, 24 upper from the class Id and 24 lower from the labels Id
              shardkey =  (GTSHelper.classId(classKeyLongs, encoder.getMetadata().getName()) & 0xFFFFFF000000L) | (GTSHelper.labelsId(labelsKeyLongs, encoder.getMetadata().getLabels()) & 0xFFFFFFL);
            }

            if (null != lastencoder) {
              this.storeClient.store(lastencoder);

              if (parseAttributes && lastHadAttributes) {
                // We need to push lastencoder's metadata update as they were updated since the last
                // metadata update message sent
                Metadata meta = new Metadata(lastencoder.getMetadata());
                if (deltaAttributes) {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
                } else {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                }
                this.directoryClient.register(meta);
                lastHadAttributes = false;
              }
            }

            if (encoder != lastencoder) {
              lastencoder = encoder;
              // This is the case when we just parsed either the first input line or one for a different
              // GTS than the previous one.
            } else {
              //lastencoder = null
              //
              // Allocate a new GTSEncoder and reuse Metadata so we can
              // correctly handle a continuation line if this is what occurs next
              //
              Metadata metadata = lastencoder.getMetadata();
              lastencoder = new GTSEncoder(0L);
              lastencoder.setMetadata(metadata);

              // This is the case when lastencoder and encoder are identical, but lastencoder was too big and needed
              // to be flushed
            }
          }
        } while (true);

        br.close();

        if (null != lastencoder && lastencoder.size() > 0) {
          // 128BITS
          lastencoder.setClassId(GTSHelper.classId(ckl0, ckl1, lastencoder.getName()));
          lastencoder.setLabelsId(GTSHelper.labelsId(lkl0, lkl1, lastencoder.getMetadata().getLabels()));

          try {
            ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId(), expose);
            ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount(), expose);
          } catch (WarpException we) {
            httpStatusCode = 429; // Too Many Requests
            throw we;
          }

          this.storeClient.store(lastencoder);

          if (parseAttributes && lastHadAttributes) {
            // Push a metadata UPDATE message so attributes are stored
            // Build metadata object to push
            Metadata meta = new Metadata(lastencoder.getMetadata());
            // Set source to indicate we
            if (deltaAttributes) {
              meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
            } else {
              meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
            }
            this.directoryClient.register(meta);
          }
        }

        //
        // TODO(hbs): should we update the count in Sensision periodically so you can't trick the throttling mechanism?
        //
      } finally {
        this.storeClient.store(null);
        this.directoryClient.register(null);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_REQUESTS, sensisionLabels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_TIME_US, sensisionLabels, (System.nanoTime() - nano) / 1000);

        //
        // Update stats with CDN
        //

        String cdn = request.getHeader(Constants.OVH_CDN_GEO_HEADER);

        if (null != cdn) {
          sensisionLabels.put(SensisionConstants.SENSISION_LABEL_CDN, cdn);
          // Per CDN stat is updated at the end, so update with 'total' + 'count'
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_DATAPOINTS_RAW, sensisionLabels, count + total);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_REQUESTS, sensisionLabels, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_TIME_US, sensisionLabels, (System.nanoTime() - nano) / 1000);
        }
      }

      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating data: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(httpStatusCode, msg);
      }
    } finally {
      WarpConfig.clearThreadProperties();
    }
  }

  /**
   * Handle Metadata updating
   */
  public void handleMeta(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_META)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }

    if (null != WarpManager.getAttribute(WarpManager.META_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.META_DISABLED)));
      return;
    }

    long lastActivity = System.currentTimeMillis();

    try {
      //
      // CORS header
      //

      response.setHeader("Access-Control-Allow-Origin", "*");

      boolean deltaAttributes = "delta".equals(request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES)));

      if (deltaAttributes && !this.allowDeltaAttributes) {
        throw new IOException("Delta update of attributes is disabled.");
      }
      //
      // Loop over the input lines.
      // Each has the following format:
      //
      // class{labels}{attributes}
      //

      String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));

      WriteToken wtoken;

      try {
        wtoken = Tokens.extractWriteToken(token);
        if (wtoken.getAttributesSize() > 0 && wtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOMETA)) {
          throw new WarpScriptException("Token cannot be used for updating metadata.");
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }

      String application = wtoken.getAppName();
      String producer = Tokens.getUUID(wtoken.getProducerId());
      String owner = Tokens.getUUID(wtoken.getOwnerId());

      if (null == producer || null == owner) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;

      if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {
        gzipped = true;
      }

      BufferedReader br = null;

      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {
        br = request.getReader();
      }

      try {
        //
        // Loop on all lines
        //

        while(true) {
          String line = br.readLine();

          if (null == line) {
            break;
          }

          // Ignore blank lines
          if ("".equals(line)) {
            continue;
          }

          // Ignore comments
          if ('#' == line.charAt(0)) {
            continue;
          }

          Metadata metadata = MetadataUtils.parseMetadata(line);

          // Add labels from the WriteToken if they exist
          if (wtoken.getLabelsSize() > 0) {
            metadata.getLabels().putAll(wtoken.getLabels());
          }
          //
          // Force owner/producer
          //

          metadata.getLabels().put(Constants.PRODUCER_LABEL, producer);
          metadata.getLabels().put(Constants.OWNER_LABEL, owner);

          if (null != application) {
            metadata.getLabels().put(Constants.APPLICATION_LABEL, application);
          } else {
            // remove application label
            metadata.getLabels().remove(Constants.APPLICATION_LABEL);
          }

          if (!MetadataUtils.validateMetadata(metadata)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + metadata);
            return;
          }

          if (deltaAttributes) {
            metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
          } else {
            metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
          }

          if (metaActivity) {
            metadata.setLastActivity(lastActivity);
          }

          if (null != this.plugin) {
            if (!this.plugin.meta(this, wtoken, line, metadata)) {
              continue;
            }
          }
          this.directoryClient.register(metadata);
        }
      } finally {
        this.directoryClient.register(null);
      }

      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating meta: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        return;
      }
    }
  }

  public IngressPlugin getPlugin() {
    return this.plugin;
  }
}
