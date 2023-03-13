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

package io.warp10.script.functions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutablePeriod;
import org.joda.time.Period;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.ISOPeriodFormat;

import io.warp10.WarpDist;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.gts.MetadataSelectorMatcher;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.MetaSet;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.unary.TOTIMESTAMP;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.AcceleratorConfig;

/**
 * Fetch GeoTimeSeries from the Warp 10 Storage Engine
 *
 * The top of the stack must contain a list of the following parameters
 *
 * @param token The token to use for data retrieval
 * @param classSelector  Class selector.
 * @param labelsSelectors Map of label name to label selector.
 * @param now Most recent timestamp to consider (in us since the Epoch)
 * @param timespan Width of time period to consider (in us). Timestamps at or before now - timespan will be ignored.
 *
 * The last two parameters can be replaced by String parameters representing the end and start ISO8601 timestamps
 */
public class FETCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String PARAM_CLASS = "class";

  /**
   * Extra classes to retrieve after Directory have been called
   */
  public static final String PARAM_EXTRA = "extra";
  public static final String PARAM_LABELS = "labels";
  public static final String PARAM_SELECTOR = "selector";
  public static final String PARAM_SELECTORS = "selectors";
  public static final String PARAM_SELECTOR_PAIRS = "selpairs";
  public static final String PARAM_TOKEN = "token";
  public static final String PARAM_END = "end";
  public static final String PARAM_START = "start";
  public static final String PARAM_COUNT = "count";
  public static final String PARAM_TIMESPAN = "timespan";
  public static final String PARAM_TYPE = "type";
  public static final String PARAM_WRITE_TIMESTAMP = "wtimestamp";
  public static final String PARAM_TTL = "ttl";
  public static final String PARAM_SHOWUUID = "showuuid";
  public static final String PARAM_TYPEATTR = "typeattr";
  public static final String PARAM_METASET = "metaset";
  public static final String PARAM_GTS = "gts";
  public static final String PARAM_ACTIVE_AFTER = "active.after";
  public static final String PARAM_QUIET_AFTER = "quiet.after";
  public static final String PARAM_BOUNDARY_PRE = "boundary.pre";
  public static final String PARAM_BOUNDARY_POST = "boundary.post";
  public static final String PARAM_BOUNDARY = "boundary";
  public static final String PARAM_SKIP = "skip";
  public static final String PARAM_STEP = "step";
  public static final String PARAM_TIMESTEP = "timestep";
  public static final String PARAM_SAMPLE = "sample";
  public static final String PARAM_LABELS_PRIORITY = "priority";
  public static final String PARAM_ENCODERS = "encoders";
  public static final String PARAM_MERGE = "merge";
  public static final String PARAM_GCOUNT = "gcount";
  public static final String PARAM_GSKIP = "gskip";
  public static final String PARAM_KEEPEMPTY = "keepempty";

  public static final String POSTFETCH_HOOK = "postfetch";

  public static final String NOW_PARAM_VALUE = "now";

  private final TYPE forcedType;

  private long[] SIPHASH_CLASS;
  private long[] SIPHASH_LABELS;

  private byte[] AES_METASET;

  private boolean initialized = false;

  public FETCH(String name, TYPE type) {
    super(name);
    this.forcedType = type;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (!initialized) {
      synchronized(FETCH.class) {
        KeyStore ks = null;

        ks = WarpDist.getKeyStore();

        if (null != ks) {
          this.SIPHASH_CLASS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_CLASS));
          this.SIPHASH_LABELS = SipHashInline.getKey(ks.getKey(KeyStore.SIPHASH_LABELS));
          this.AES_METASET = ks.getKey(KeyStore.AES_METASETS);
        } else {
          this.SIPHASH_CLASS = null;
          this.SIPHASH_LABELS = null;
          this.AES_METASET = null;
        }
      }
      initialized = true;
    }

    //
    // Extract parameters from the stack
    //

    Object top = stack.pop();

    //
    // Handle the new (as of 20150805) parameter passing mechanism as a map
    //

    Map<String,Object> params = null;

    if (top instanceof Map) {
      params = paramsFromMap((Map<String,Object>) top);
    } else if (top instanceof List) {
      List list = (List)top;
      if (5 != list.size()) {
        throw new WarpScriptException(getName() + " expects a list with 5 elements.");
      }

      // convert list spec to map spec
      Map<String,Object> map = new HashMap<String,Object>();
      map.put(PARAM_TOKEN, list.get(0));
      map.put(PARAM_CLASS, list.get(1));
      map.put(PARAM_LABELS, list.get(2));

      if (list.get(3) instanceof Long && list.get(4) instanceof Long) {
        map.put(PARAM_END, list.get(3));
        map.put(PARAM_TIMESPAN, list.get(4));
      } else if (list.get(3) instanceof String && list.get(4) instanceof String) {
        map.put(PARAM_START, list.get(3));
        map.put(PARAM_END, list.get(4));
      } else {
        throw new WarpScriptException(getName() + " expects '" + PARAM_START + "' and '" + PARAM_END + "' to be Strings or '" + PARAM_END + "' and '" + PARAM_TIMESPAN + "' to be Longs.");
      }

      params = paramsFromMap(map);
    } else {
      throw new  WarpScriptException(getName()+" expects a map or a list as parameter.");
    }

    StoreClient gtsStore = stack.getStoreClient();

    DirectoryClient directoryClient = stack.getDirectoryClient();

    GeoTimeSerie base = null;
    GeoTimeSerie[] bases = null;
    String typeattr = (String) params.get(PARAM_TYPEATTR);

    if (null != typeattr) {
      bases = new GeoTimeSerie[5];
    }

    ReadToken rtoken;
    try {
      rtoken = Tokens.extractReadToken(params.get(PARAM_TOKEN).toString());

      Map<String, String> rtokenAttributes = rtoken.getAttributes();
      if (null != rtokenAttributes && (rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFETCH) || rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFIND))) {
        throw new WarpScriptException("Token cannot be used for fetching data.");
      }
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " given an invalid read token.", wse);
    }

    //
    // Parse the token attributes and update params accordingly
    //

    Map<String,Object> tokenattr = parseTokenAttributes(rtoken, params);

    if (null != tokenattr.get(PARAM_START)) {
      long start = (long) tokenattr.get(PARAM_START);
      if ((long) params.get(PARAM_START) < start) {
        params.put(PARAM_START, start);
      }
    }

    if (null != tokenattr.get(PARAM_END)) {
      long end = (long) tokenattr.get(PARAM_END);
      if ((long) params.get(PARAM_END) > end) {
        params.put(PARAM_END, end);
      }
    }

    if (null != tokenattr.get(PARAM_QUIET_AFTER)) {
      long activity = (long) tokenattr.get(PARAM_QUIET_AFTER);
      params.put(PARAM_QUIET_AFTER, activity);
    }

    if (null != tokenattr.get(PARAM_ACTIVE_AFTER)) {
      long activity = (long) tokenattr.get(PARAM_ACTIVE_AFTER);
      params.put(PARAM_ACTIVE_AFTER, activity);
    }

    if (null != tokenattr.get(PARAM_COUNT)) {
      long count = (long) tokenattr.get(PARAM_COUNT);
      if ((long) params.getOrDefault(PARAM_COUNT, count) >= count) {
        params.put(PARAM_COUNT, count);
      }
    }

    if (null != tokenattr.get(PARAM_BOUNDARY_PRE)) {
      long boundary = (long) tokenattr.get(PARAM_BOUNDARY_PRE);
      if ((long) params.getOrDefault(PARAM_BOUNDARY_PRE, boundary) >= boundary) {
        params.put(PARAM_BOUNDARY_PRE, boundary);
      }
    }

    if (null != tokenattr.get(PARAM_BOUNDARY_POST)) {
      long boundary = (long) tokenattr.get(PARAM_BOUNDARY_POST);
      if ((long) params.getOrDefault(PARAM_BOUNDARY_POST, boundary) >= boundary) {
        params.put(PARAM_BOUNDARY_POST, boundary);
      }
    }

    boolean expose = rtoken.getAttributesSize() > 0 && rtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);

    boolean merge = true;

    if (Boolean.FALSE.equals(params.get(PARAM_MERGE))) {
      merge = false;
    }

    boolean asEncoders = false;

    if (Boolean.TRUE.equals(params.get(PARAM_ENCODERS))) {
      asEncoders = true;
    }

    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();

    MetaSet metaset = null;

    List<Metadata> metadatas = null;
    Iterator<Metadata> iter = null;

    long gskip = 0L;
    long gcount = Long.MAX_VALUE;

    if (params.get(PARAM_GSKIP) instanceof Long) {
      gskip = ((Long) params.get(PARAM_GSKIP)).longValue();
    }

    if (params.get(PARAM_GCOUNT) instanceof Long) {
      gcount = ((Long) params.get(PARAM_GCOUNT)).longValue();
    }

    if (params.containsKey(PARAM_METASET)) {
      metaset = (MetaSet) params.get(PARAM_METASET);

      // metaset is sorted by gts id so we do not need to sort it further
      iter = metaset.getMetadatas().iterator();
    } else if (params.containsKey(PARAM_GTS)) {
      List<Metadata> metas = (List<Metadata>) params.get(PARAM_GTS);

      Map<String,String> tokenSelectors = Tokens.labelSelectorsFromReadToken(rtoken);

      boolean singleApp = tokenSelectors.containsKey(Constants.APPLICATION_LABEL) && '=' == tokenSelectors.get(Constants.APPLICATION_LABEL).charAt(0);
      boolean singleOwner = tokenSelectors.containsKey(Constants.OWNER_LABEL) && '=' == tokenSelectors.get(Constants.OWNER_LABEL).charAt(0);
      boolean singleProducer = tokenSelectors.containsKey(Constants.PRODUCER_LABEL) && '=' == tokenSelectors.get(Constants.PRODUCER_LABEL).charAt(0);

      String application = singleApp ? tokenSelectors.get(Constants.APPLICATION_LABEL).substring(1) : null;
      String owner = singleOwner ? tokenSelectors.get(Constants.OWNER_LABEL).substring(1) : null;
      String producer = singleProducer ? tokenSelectors.get(Constants.PRODUCER_LABEL).substring(1) : null;

      Metadata tmeta = new Metadata();
      tmeta.setName("");
      tmeta.setLabels(tokenSelectors);

      // Build a selector matching all classes
      String tselector = "~.*" + GTSHelper.buildSelector(tmeta, true);
      MetadataSelectorMatcher matcher = new MetadataSelectorMatcher(tselector);

      //
      // Build a selector
      for (Metadata m: metas) {
        if (null == m.getLabels()) {
          m.setLabels(new LinkedHashMap<String,String>());
        }

        //
        // If the Metadata have producer/owner/app labels, check if 'matcher' would select them
        //

        boolean matches = false;

        if (m.getLabels().containsKey(Constants.PRODUCER_LABEL)
            && m.getLabels().containsKey(Constants.OWNER_LABEL)
            && m.getLabels().containsKey(Constants.APPLICATION_LABEL)) {
          matches = matcher.matches(m);
        }

        //
        // If the metadata would not get selected by the provided token
        // force the producer/owner/app to be that of the token
        //

        if (!matches) {
          //
          // We will now set producer/owner/application
          //

          //
          // If the token doesn't contain a single app we abort the selection as we cannot
          // choose an app which would be within the reach of the token
          //

          if (singleApp) {
            m.getLabels().put(Constants.APPLICATION_LABEL, application);
          } else {
            throw new WarpScriptException(getName() + " provided token is incompatible with '" + PARAM_GTS + "' parameter, expecting a single application.");
          }

          if (singleProducer && singleOwner) {
            //
            // If the token has a single producer and single owner, use them for the GTS
            //
            m.getLabels().put(Constants.PRODUCER_LABEL, producer);
            m.getLabels().put(Constants.OWNER_LABEL, owner);
          } else if (singleProducer && !tokenSelectors.containsKey(Constants.OWNER_LABEL)) {
            //
            // If the token has a single producer but no owner, use the producer as the owner, this would
            // lead to a narrower scope than what the token would actually select so it is fine.
            //
            m.getLabels().put(Constants.PRODUCER_LABEL, producer);
            m.getLabels().put(Constants.OWNER_LABEL, producer);
          } else if (singleOwner && !tokenSelectors.containsKey(Constants.PRODUCER_LABEL)) {
            //
            // If the token has a single owner but no producer, use the owner as the producer, again this would
            // lead to a narrower scope than what the token can actually access so it is fine too.
            //
            m.getLabels().put(Constants.OWNER_LABEL, owner);
            m.getLabels().put(Constants.PRODUCER_LABEL, owner);
          } else {
            throw new WarpScriptException(getName() + " provided token is incompatible with '" + PARAM_GTS + "' parameter, expecting a single producer and/or single owner.");
          }
        }

        // Recompute IDs
        m.setClassId(GTSHelper.classId(this.SIPHASH_CLASS, m.getName()));
        m.setLabelsId(GTSHelper.labelsId(this.SIPHASH_LABELS, m.getLabels()));
      }

      //
      // Sort metadata by id so the enforcement of keepempty works
      //

      Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);

      iter = ((List<Metadata>) params.get(PARAM_GTS)).iterator();
    } else {
      if (params.containsKey(PARAM_SELECTOR_PAIRS)) {
        for (Pair<Object,Object> pair: (List<Pair<Object,Object>>) params.get(PARAM_SELECTOR_PAIRS)) {
          clsSels.add(pair.getLeft().toString());
          Map<String,String> labelSelectors = (Map<String,String>) pair.getRight();
          labelSelectors.remove(Constants.PRODUCER_LABEL);
          labelSelectors.remove(Constants.OWNER_LABEL);
          labelSelectors.remove(Constants.APPLICATION_LABEL);
          labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

          // Re-order the labels
          List<String> order = null;
          if (params.containsKey(PARAM_LABELS_PRIORITY)) {
            order = (List<String>) params.get(PARAM_LABELS_PRIORITY);
          } else {
            order = FIND.DEFAULT_LABELS_PRIORITY;
          }
          Map<String,String> ordered = new LinkedHashMap<String,String>(labelSelectors.size());
          for (String label: order) {
            if (labelSelectors.containsKey(label)) {
              ordered.put(label, labelSelectors.get(label));
            }
          }
          for (Entry<String,String> entry: labelSelectors.entrySet()) {
            if (order.contains(entry.getKey())) {
              continue;
            }
            ordered.put(entry.getKey(), entry.getValue());
          }

          lblsSels.add(ordered);
        }
      } else {
        clsSels.add(params.get(PARAM_CLASS).toString());

        Map<String, String> labelSelectors = new LinkedHashMap<>((Map<String, String>) params.get(PARAM_LABELS));
        labelSelectors.remove(Constants.PRODUCER_LABEL);
        labelSelectors.remove(Constants.OWNER_LABEL);
        labelSelectors.remove(Constants.APPLICATION_LABEL);
        labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

        // Re-order the labels
        List<String> order = null;
        if (params.containsKey(PARAM_LABELS_PRIORITY)) {
          order = (List<String>) params.get(PARAM_LABELS_PRIORITY);
        } else {
          order = FIND.DEFAULT_LABELS_PRIORITY;
        }
        Map<String,String> ordered = new LinkedHashMap<String,String>(labelSelectors.size());
        for (String label: order) {
          if (labelSelectors.containsKey(label)) {
            ordered.put(label, labelSelectors.get(label));
          }
        }
        for (Entry<String,String> entry: labelSelectors.entrySet()) {
          if (order.contains(entry.getKey())) {
            continue;
          }
          ordered.put(entry.getKey(), entry.getValue());
        }

        lblsSels.add((Map<String,String>) ordered);
      }

      DirectoryRequest drequest = new DirectoryRequest();
      drequest.setClassSelectors(clsSels);
      drequest.setLabelsSelectors(lblsSels);

      if (params.containsKey(PARAM_ACTIVE_AFTER)) {
        drequest.setActiveAfter((long) params.get(PARAM_ACTIVE_AFTER));
      }

      if (params.containsKey(PARAM_QUIET_AFTER)) {
        drequest.setQuietAfter((long) params.get(PARAM_QUIET_AFTER));
      }

      try {
        metadatas = directoryClient.find(drequest);
        iter = metadatas.iterator();
      } catch (IOException ioe) {
        try {
          iter = directoryClient.iterator(drequest);
        } catch (Exception e) {
          throw new WarpScriptException(getName() + " failed.", e);
        }
      }
    }

    boolean keepempty = Boolean.TRUE.equals(params.get(PARAM_KEEPEMPTY));

    metadatas = new ArrayList<Metadata>();

    List<Object> series = new ArrayList<Object>();
    List<Object> batch = new ArrayList<Object>();

    AtomicLong fetched = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT);
    long fetchLimit = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT);
    long gtsLimit = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT);

    AtomicLong gtscount = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_GTS_COUNT);

    // Variables to keep track of the last Metadata and fetched count
    Metadata lastMetadata = null;
    long lastCount = 0L;

    GTSEncoder lastEncoder = null;

    long preBoundary = 0;
    long postBoundary = 0;

    if (params.containsKey(PARAM_BOUNDARY_PRE)) {
      preBoundary = (long) params.get(PARAM_BOUNDARY_PRE);
    }
    if (params.containsKey(PARAM_BOUNDARY_POST)) {
      postBoundary = (long) params.get(PARAM_BOUNDARY_POST);
    }

    try {
      Thread thread = Thread.currentThread();

      while(iter.hasNext() && !thread.isInterrupted()) {
        if (gcount <= 0) {
          break;
        }

        Metadata m = iter.next();

        if (gskip > 0) {
          gskip--;
          continue;
        }

        gcount--;

        metadatas.add(m);

        if (gtscount.incrementAndGet() > gtsLimit) {
          throw new WarpScriptException(getName() + " exceeded limit of " + gtsLimit + " Geo Time Series, current count is " + gtscount);
        }

        stack.handleSignal();

        if (metadatas.size() < EgressFetchHandler.FETCH_BATCHSIZE && gcount > 0 && iter.hasNext()) {
          continue;
        }

        //
        // Generate extra Metadata if PARAM_EXTRA is set.
        // Those series are not counted towards gskip/gcount
        //

        if (params.containsKey(PARAM_EXTRA)) {

          Set<Metadata> withextra = new LinkedHashSet<Metadata>();

          withextra.addAll(metadatas);

          for (Metadata meta: metadatas) {
            for (String cls: (Set<String>) params.get(PARAM_EXTRA)) {
              // The following is safe, the constructor allocates new maps
              Metadata metadata = new Metadata(meta);
              metadata.setName(cls);
              metadata.setClassId(GTSHelper.classId(this.SIPHASH_CLASS, cls));
              metadata.setLabelsId(GTSHelper.labelsId(this.SIPHASH_LABELS, metadata.getLabels()));
              withextra.add(metadata);
            }
          }

          metadatas.clear();
          metadatas.addAll(withextra);
        }

        //
        // We assume that GTS will be fetched in a continuous way, i.e. without having a GTSDecoder from one
        // then one from another, then one from the first one.
        //

        long count = -1L;

        if (params.containsKey(PARAM_COUNT)) {
          count = (long) params.get(PARAM_COUNT);
        }

        long then = (long) params.get(PARAM_START);
        long skip = (long) params.getOrDefault(PARAM_SKIP, 0L);
        long timestep = 1L;
        long step = 1L;

        if (params.containsKey(PARAM_TIMESTEP)) {
          timestep = (long) params.get(PARAM_TIMESTEP);
        }

        if (params.containsKey(PARAM_STEP)) {
          step = (long) params.get(PARAM_STEP);
        }

        double sample = (double) params.getOrDefault(PARAM_SAMPLE, 1.0D);

        TYPE type = (TYPE) params.get(PARAM_TYPE);

        if (null != this.forcedType) {
          if (null != type) {
            throw new WarpScriptException(getName() + " type of fetched GTS cannot be changed.");
          }
          type = this.forcedType;
        }

        boolean writeTimestamp = Boolean.TRUE.equals(params.get(PARAM_WRITE_TIMESTAMP));
        boolean ttl = Boolean.TRUE.equals(params.get(PARAM_TTL));

        boolean showUUID = Boolean.TRUE.equals(params.get(PARAM_SHOWUUID));

        TYPE lastType = TYPE.UNDEFINED;

        long end = (long) params.get(PARAM_END);

        boolean nocache = AcceleratorConfig.getDefaultReadNocache();
        if (null != stack.getAttribute(AcceleratorConfig.ATTR_NOCACHE)) {
          nocache = Boolean.TRUE.equals(stack.getAttribute(AcceleratorConfig.ATTR_NOCACHE));
        }
        boolean nopersist = AcceleratorConfig.getDefaultReadNopersist();
        if (null != stack.getAttribute(AcceleratorConfig.ATTR_NOPERSIST)) {
          nopersist = Boolean.TRUE.equals(stack.getAttribute(AcceleratorConfig.ATTR_NOPERSIST));
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

        // Flag indicating the FETCH is a count only, no pre/post boundaries
        boolean countOnly = count >= 0 && 0 == preBoundary && 0 == postBoundary;

        FetchRequest req = new FetchRequest();
        req.setToken(rtoken);
        req.setMetadatas(metadatas);
        req.setNow(end);
        req.setThents(then);
        req.setCount(count);
        req.setSkip(skip);
        req.setStep(step);
        req.setTimestep(timestep);
        req.setSample(sample);
        req.setPreBoundary(preBoundary);
        req.setPostBoundary(postBoundary);

        int nseries = series.size();
        batch.clear();

        try (GTSDecoderIterator gtsiter = gtsStore.fetch(req)) {
          while(gtsiter.hasNext() && !thread.isInterrupted()) {
            GTSDecoder decoder = gtsiter.next();

            //
            // Handle retrieval of encoders
            //
            if (asEncoders) {
              decoder.next();
              GTSEncoder encoder = decoder.getEncoder(false);

              //
              // Add a .uuid attribute if instructed to do so
              //

              if (showUUID) {
                java.util.UUID uuid = new java.util.UUID(encoder.getMetadata().getClassId(), encoder.getMetadata().getLabelsId());
                encoder.getMetadata().putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
              }

              Map<String,String> labels = new LinkedHashMap<String, String>();
              labels.putAll(encoder.getMetadata().getLabels());

              if (!Constants.EXPOSE_OWNER_PRODUCER && !expose) {
                labels.remove(Constants.PRODUCER_LABEL);
                labels.remove(Constants.OWNER_LABEL);
              }
              encoder.setLabels(labels);

              if (!encoder.getMetadata().equals(lastMetadata)) {
                lastMetadata = encoder.getMetadata();
                lastCount = 0L;
              }

              // Do we need to shrink encoder?
              if (countOnly && lastCount + encoder.getCount() > count) {
                decoder = encoder.getDecoder(true);
                encoder = decoder.getCompatibleEncoder(encoder.getBaseTimestamp());
                // Copy just enough values
                long copy = count - lastCount;
                while(copy > 0 && decoder.next()) {
                  encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                  copy--;
                }
              }

              lastCount += encoder.getCount();

              if (merge) {
                if (null == lastEncoder || !lastEncoder.getName().equals(encoder.getName()) || !lastEncoder.getLabels().equals(encoder.getLabels())) {
                  lastEncoder = encoder;
                  batch.add(lastEncoder);
                } else {
                  lastEncoder.merge(encoder);
                }
              } else {
                batch.add(encoder);
              }

              if (fetched.addAndGet(encoder.getCount()) > fetchLimit) {
                Map<String,String> sensisionLabels = new HashMap<String, String>();
                sensisionLabels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, Tokens.getUUID(rtoken.getBilledId()));
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FETCHCOUNT_EXCEEDED, sensisionLabels, 1);
                throw new WarpScriptException(getName() + " exceeded limit of " + fetchLimit + " datapoints, current count is " + fetched.get());
              }

              continue;
            }

            boolean identical = true;

            if (null == lastMetadata || !lastMetadata.equals(decoder.getMetadata())) {
              lastMetadata = decoder.getMetadata();
              identical = false;
              lastCount = 0;
              lastType = TYPE.UNDEFINED;
            }

            GeoTimeSerie gts;

            //
            // If we should ventilate per type, do so now
            //

            if (null != typeattr) {

              java.util.UUID uuid = null;

              if (showUUID) {
                uuid = new java.util.UUID(decoder.getClassId(), decoder.getLabelsId());
              }

              long dpcount = 0;

              Metadata decoderMeta = new Metadata(decoder.getMetadata());
              // Remove producer/owner labels
              if (!Constants.EXPOSE_OWNER_PRODUCER && !expose) {
                decoderMeta.getLabels().remove(Constants.PRODUCER_LABEL);
                decoderMeta.getLabels().remove(Constants.OWNER_LABEL);
              }

              while(decoder.next()) {
                long ts = decoder.getTimestamp();
                long location = decoder.getLocation();
                long elevation = decoder.getElevation();
                Object value = decoder.getBinaryValue();


                dpcount++;

                int gtsidx = 0;
                String typename = "DOUBLE";

                if (value instanceof Long) {
                  gtsidx = 1;
                  typename = "LONG";
                } else if (value instanceof Boolean) {
                  gtsidx = 2;
                  typename = "BOOLEAN";
                } else if (value instanceof String) {
                  gtsidx = 3;
                  typename = "STRING";
                } else if (value instanceof byte[]) {
                  gtsidx = 4;
                  typename = "BINARY";
                }

                base = bases[gtsidx];

                if (null == base || !base.getMetadata().getName().equals(decoderMeta.getName()) || !base.getMetadata().getLabels().equals(decoderMeta.getLabels())) {
                  bases[gtsidx] = new GeoTimeSerie();
                  base = bases[gtsidx];
                  batch.add(base);
                  // Copy labels to GTS, producer and owner have already been removed
                  base.setMetadata(decoderMeta);

                  // Force type attribute
                  base.getMetadata().putToAttributes(typeattr, typename);
                  if (null != uuid) {
                    base.getMetadata().putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
                  }
                }

                //
                // When using HBase, fetch requests which do not have boundaries may be served
                // using a custom filter. This filter may return more data for a GTS than the requested
                // count if the GTS spans multiple regions because the filtering is performed on the Region Servers
                // and in this specific case the request will be forwarded to all RS serving regions for a given GTS.
                // It is therefore necessary to keep track of how many data points were already fetched and
                // shrink the GTS so we do not return too many.
                //
                // TODO(hbs): Even though we migrated to using FoundationDB, this test remains in place for the time being, to be assessed and possibly removed later
                //

                if (countOnly && lastCount + dpcount >= count) {
                  // We are done, exit
                  break;
                }

                GTSHelper.setValue(base, ts, location, elevation, value, false);
              }

              if (fetched.addAndGet(dpcount) > fetchLimit) {
                Map<String,String> sensisionLabels = new HashMap<String, String>();
                sensisionLabels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, Tokens.getUUID(rtoken.getBilledId()));
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FETCHCOUNT_EXCEEDED, sensisionLabels, 1);
                throw new WarpScriptException(getName() + " exceeded limit of " + fetchLimit + " datapoints, current count is " + fetched.get());
              }

              stack.handleSignal();

              lastCount += dpcount;

              continue;
            }

            if (null != type) {
              gts = decoder.decode(type);
            } else {
              //
              // We need to decode using the same type as the previous decoder for the same GTS
              // Otherwise, if it happens that the current decoder starts with a value of another
              // type then the merge will not take into account this decoder as the decoded GTS
              // will be of a different type.
              if (identical && lastType != TYPE.UNDEFINED) {
                gts = decoder.decode(lastType);
              } else {
                gts = decoder.decode();
              }
              lastType = gts.getType();
            }

            //
            // When using HBase, fetch requests which do not have boundaries may be served
            // using a custom filter. This filter may return more data for a GTS than the requested
            // count if the GTS spans multiple regions because the filtering is performed on the Region Servers
            // and in this specific case the request will be forwarded to all RS serving regions for a given GTS.
            // It is therefore necessary to keep track of how many datapoints were already fetched and
            // shrink the GTS so we do not return too many.
            //
            // TODO(hbs): Even though we migrated to using FoundationDB, this test remains in place for the time being, to be assessed and possibly removed later
            //

            if (countOnly && lastCount + GTSHelper.nvalues(gts) > count) {
              gts = GTSHelper.shrinkTo(gts, (int) Math.max(count - lastCount, 0));
            }

            lastCount += GTSHelper.nvalues(gts);

            //
            // Remove producer/owner labels
            //

            //
            // Add a .uuid attribute if instructed to do so
            //

            if (showUUID) {
              java.util.UUID uuid = new java.util.UUID(gts.getClassId(), gts.getLabelsId());
              gts.getMetadata().putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
            }

            Map<String,String> labels = new LinkedHashMap<String, String>();
            labels.putAll(gts.getMetadata().getLabels());

            if (!Constants.EXPOSE_OWNER_PRODUCER && !expose) {
              labels.remove(Constants.PRODUCER_LABEL);
              labels.remove(Constants.OWNER_LABEL);
            }
            gts.setLabels(labels);

            //
            // If it's the first GTS, take it as is.
            //

            if (null == base) {
              base = gts;
            } else {
              //
              // If name and labels are identical to the previous GTS, merge them
              // Otherwise add 'base' to the stack and set it to 'gts'.
              //
              if (!merge || !base.getMetadata().getName().equals(gts.getMetadata().getName()) || !base.getMetadata().getLabels().equals(gts.getMetadata().getLabels())) {
                batch.add(base);
                base = gts;
              } else {
                base = GTSHelper.merge(base, gts);
              }
            }

            if (fetched.addAndGet(gts.size()) > fetchLimit) {
              Map<String,String> sensisionLabels = new HashMap<String, String>();
              sensisionLabels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, Tokens.getUUID(rtoken.getBilledId()));
              Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FETCHCOUNT_EXCEEDED, sensisionLabels, 1);
              throw new WarpScriptException(getName() + " exceeded limit of " + fetchLimit + " datapoints, current count is " + fetched.get());
              //break;
            }

            stack.handleSignal();
          }
        } catch (WarpScriptException ee) {
          throw ee;
        } catch (Throwable t) {
          throw new WarpScriptException(getName() + " failed.", t);
        }


        //
        // If there is one current GTS, push it onto the stack (only if not ventilating per type)
        //

        if (null != base && null == typeattr) {
          batch.add(base);
        }

        //
        // Reset state
        //

        base = null;

        //
        // Clean up series according to keepempty
        //

        int j = 0;

        if (!batch.isEmpty()) {
          for (int i = 0; i < batch.size(); i++) {
            Object o = batch.get(i);
            boolean ignore = false;
            Metadata meta = null;

            if (o instanceof GeoTimeSerie) {
              GeoTimeSerie g = (GeoTimeSerie) o;
              meta = g.getMetadata();

              if (!keepempty && 0 == GTSHelper.nvalues(g)) {
                ignore = true;
              }
            } else { // GTSEncoder
              GTSEncoder e = (GTSEncoder) o;
              meta = e.getRawMetadata();
              if (!keepempty && 0 == e.getCount()) {
                ignore = true;
              }
            }

            // Check if there are some Metadata with no data yet
            if (keepempty) {
              while(MetadataUtils.compare(metadatas.get(j), meta) < 0) {
                if (asEncoders) {
                  GTSEncoder e = new GTSEncoder(0L);
                  e.setMetadata(meta);
                  series.add(e);
                } else {
                  GeoTimeSerie g = new GeoTimeSerie();
                  g.setMetadata(meta);
                  series.add(g);
                }

                j++;
              }
            }

            if (!ignore) {
              series.add(o);
            }
          }
        } else {
          if (keepempty) {
            for (Metadata meta: metadatas) {
              if (asEncoders) {
                GTSEncoder e = new GTSEncoder(0L);
                e.setMetadata(meta);
                series.add(e);
              } else {
                GeoTimeSerie g = new GeoTimeSerie();
                g.setMetadata(meta);
                series.add(g);
              }
            }
          }
        }

        metadatas.clear();
      }
    } catch (Throwable t) {
      throw t;
    } finally {
      if (iter instanceof MetadataIterator) {
        try {
          ((MetadataIterator) iter).close();
        } catch (Exception e) {
        }
      }
    }

    stack.push(series);

    //
    // Apply a possible postfetch hook
    //

    if (rtoken.getHooksSize() > 0 && rtoken.getHooks().containsKey(POSTFETCH_HOOK)) {
      stack.execMulti(rtoken.getHooks().get(POSTFETCH_HOOK));
    }

    return stack;
  }

  public Map<String,Object> paramsFromMap(Map<String,Object> map) throws WarpScriptException {
    Map<String,Object> params = new HashMap<String, Object>();

    //
    // Handle the case where a MetaSet was passed as this will
    // modify some other parameters
    //

    MetaSet metaset = null;

    if (map.containsKey(PARAM_METASET)) {

      if (null == AES_METASET) {
        throw new WarpScriptException(getName() + " MetaSet support not available.");
      }

      Object ms = map.get(PARAM_METASET);

      if (!(ms instanceof byte[])) {
        // Decode
        byte[] decoded = OrderPreservingBase64.decode(ms.toString().getBytes(StandardCharsets.US_ASCII));

        // Decrypt
        byte[] decrypted = CryptoUtils.unwrap(AES_METASET, decoded);

        // Decompress

        try {
          ByteArrayOutputStream out = new ByteArrayOutputStream(decrypted.length);
          InputStream in = new GZIPInputStream(new ByteArrayInputStream(decrypted));

          byte[] buf = new byte[1024];

          while(true) {
            int len = in.read(buf);
            if (len < 0) {
              break;
            }
            out.write(buf, 0, len);
          }

          in.close();
          out.close();

          ms = out.toByteArray();
        } catch (IOException e) {
          throw new WarpScriptException(getName() + " encountered an invalid MetaSet.", e);
        }
      }

      metaset = new MetaSet();
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

      try {
        deser.deserialize(metaset, (byte[]) ms);
      } catch (TException te) {
        throw new WarpScriptException(getName() + " was unable to decode the provided MetaSet.", te);
      }

      //
      // Check if MetaSet has expired
      //

      if (metaset.getExpiry() < System.currentTimeMillis()) {
        throw new WarpScriptException(getName() + " MetaSet has expired.");
      }

      // Attempt to extract token, this will raise an exception if token has expired or was revoked
      try {
        ReadToken rtoken = Tokens.extractReadToken(metaset.getToken());

        Map<String, String> rtokenAttributes = rtoken.getAttributes();
        if (null != rtokenAttributes && (rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFETCH) || rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFIND))) {
          throw new WarpScriptException("Token cannot be used for fetching data.");
        }
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " MetaSet token is not valid.", wse);
      }

      params.put(PARAM_METASET, metaset);
      params.put(PARAM_TOKEN, metaset.getToken());
    }

    if (!params.containsKey(PARAM_TOKEN)) {
      if (!map.containsKey(PARAM_TOKEN)) {
        throw new WarpScriptException(getName() + " Missing '" + PARAM_TOKEN + "' parameter");
      }

      params.put(PARAM_TOKEN, map.get(PARAM_TOKEN));
    }

    if (map.containsKey(PARAM_GTS)) {
      Object o = map.get(PARAM_GTS);

      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " invalid '" + PARAM_GTS + "' parameter, expected a list of Geo Time Series.");
      }

      List<Metadata> metadatas = new ArrayList<Metadata>();

      for (Object elt: (List<Object>) o) {
        if (!(elt instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " invalid '" + PARAM_GTS + "' parameter, expected a list of Geo Time Series.");
        }
        metadatas.add((new Metadata(((GeoTimeSerie) elt).getMetadata())));
      }

      params.put(PARAM_GTS, metadatas);
    }

    if (map.containsKey(PARAM_SELECTORS)) {
      Object sels = map.get(PARAM_SELECTORS);
      if (!(sels instanceof List)) {
        throw new WarpScriptException(getName() + " Invalid parameter '" + PARAM_SELECTORS + "'");
      }
      List<Pair<Object, Object>> selectors = new ArrayList<Pair<Object,Object>>();

      for (Object sel: (List) sels) {
        Object[] clslbls = PARSESELECTOR.parse(sel.toString());
        selectors.add(Pair.of(clslbls[0], clslbls[1]));
      }
      params.put(PARAM_SELECTOR_PAIRS, selectors);
    } else if (map.containsKey(PARAM_SELECTOR)) {
      Object[] clslbls = PARSESELECTOR.parse(map.get(PARAM_SELECTOR).toString());
      params.put(PARAM_CLASS, clslbls[0]);
      params.put(PARAM_LABELS, clslbls[1]);
    } else if (map.containsKey(PARAM_CLASS) && map.containsKey(PARAM_LABELS)) {
      params.put(PARAM_CLASS, map.get(PARAM_CLASS));
      params.put(PARAM_LABELS, map.get(PARAM_LABELS));
    } else if (!params.containsKey(PARAM_METASET) && !params.containsKey(PARAM_GTS)) {
      throw new WarpScriptException(getName() + " Missing '" + PARAM_METASET + "', '" + PARAM_GTS + "', '" + PARAM_SELECTOR + "', '" + PARAM_SELECTORS + "' or '" + PARAM_CLASS + "' and '" + PARAM_LABELS + "' parameters.");
    }

    //
    // Time range and count specifications
    //

    // Handle negative timestamp as alias of count
    // In that case, remove timespan spec and add count spec.
    if (map.get(PARAM_TIMESPAN) instanceof Long && (long) map.get(PARAM_TIMESPAN) < 0) {
      if (map.containsKey(PARAM_COUNT)) {
        throw new WarpScriptException(getName() + " cannot be given both '" + PARAM_COUNT + "' and negative '" + PARAM_TIMESPAN + "'.");
      } else {
        long timespan = (long) map.get(PARAM_TIMESPAN);
        // Make sure negation will be positive
        if (Long.MIN_VALUE == timespan) {
          timespan++; // It's ok to modify by one the count of points when fetching -Long.MIN_VALUE points
        }
        map.put(PARAM_COUNT, -timespan);
        map.remove(PARAM_TIMESPAN);
      }
    }

    if (map.containsKey(PARAM_COUNT)) {
      params.put(PARAM_COUNT, map.get(PARAM_COUNT));
    }

    try {
      Long[] timeRange = computeTimeRange(map.get(PARAM_START), PARAM_START, map.get(PARAM_END), PARAM_END, map.get(PARAM_TIMESPAN), PARAM_TIMESPAN, map.get(PARAM_COUNT), PARAM_COUNT);
      params.put(PARAM_START, timeRange[0]);
      params.put(PARAM_END, timeRange[1]);
      if (null != timeRange[2]) {
        // Only useful for MetaSet timespan check
        params.put(PARAM_TIMESPAN, timeRange[2]);
      }
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " given invalid parameters.", wse);
    }

    //
    // Check time range and count against MetaSet, adjust limits accordingly
    //

    if (null != metaset) {
      // Metaset is incompatible with pre/post boundaries
      if (map.containsKey(PARAM_BOUNDARY_PRE) || map.containsKey((PARAM_BOUNDARY_POST))) {
        throw new WarpScriptException(getName() + " cannot support both MetaSet and pre/post boundary parameters.");
      }

      if (metaset.isSetMaxduration()) {
        // Force 'end' to 'now' only if there are no 'notbefore' and no 'notafter'.
        if (!metaset.isSetNotbefore() && !metaset.isSetNotafter()) {
          params.put(PARAM_END, TimeSource.getTime());
        }

        // Metaset limits timespan
        if (metaset.getMaxduration() >= 0) {
          // Set timespan to limit if not defined or greater than limit
          if (!params.containsKey(PARAM_TIMESPAN) || (long) params.get(PARAM_TIMESPAN) > metaset.getMaxduration()) {
            params.put(PARAM_TIMESPAN, metaset.getMaxduration());
          }
        }

        // Metaset limits count
        if (metaset.getMaxduration() < 0) {
          // Set count to limit if not defined or greater than limit
          if (!params.containsKey(PARAM_COUNT) || (long) params.get(PARAM_COUNT) > -metaset.getMaxduration()) {
            params.put(PARAM_COUNT, -metaset.getMaxduration());
          }
        }
      }

      // Limit end to 'notbefore'.
      if (metaset.isSetNotbefore() && (long) params.get(PARAM_END) < metaset.getNotbefore()) {
        params.put(PARAM_END, metaset.getNotbefore());
      }

      // Limit end to 'notafter'.
      if (metaset.isSetNotafter() && (long) params.get(PARAM_END) > metaset.getNotafter()) {
        params.put(PARAM_END, metaset.getNotafter());
      }

      // If the fetch uses a timespan or if this metaset enforce it, recompute start because end or timespan may have been changed.
      if(params.containsKey(PARAM_TIMESPAN)) {
        try {
          // Check edge case
          if (0 == (long) params.get(PARAM_TIMESPAN) && Long.MAX_VALUE == (long) params.get(PARAM_END)) {
            throw new WarpScriptException(getName() + " use of MetaSet restrictions make it so '" + PARAM_TIMESPAN + "' is 0 and '" + PARAM_START + "' is MIN_VALUE, which is not supported.");
          }
          long newStart = Math.subtractExact((long) params.get(PARAM_END), (long) params.get(PARAM_TIMESPAN)) + 1;
          params.put(PARAM_START, newStart);
        } catch (ArithmeticException ae) {
          params.put(PARAM_START, Long.MIN_VALUE);
        }
      }
    }

    if (map.containsKey(PARAM_TYPE)) {
      String type = map.get(PARAM_TYPE).toString();

      if (TYPE.LONG.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.LONG);
      } else if (TYPE.DOUBLE.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.DOUBLE);
      } else if (TYPE.STRING.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.STRING);
      } else if (TYPE.BOOLEAN.name().equalsIgnoreCase(type)) {
        params.put(PARAM_TYPE, TYPE.BOOLEAN);
      } else {
        throw new WarpScriptException(getName() + " Invalid value for parameter '" + PARAM_TYPE + "'.");
      }
    }

    if (map.containsKey(PARAM_TYPEATTR)) {
      if (map.containsKey(PARAM_TYPE)) {
        throw new WarpScriptException(getName() + " Incompatible parameters '" +  PARAM_TYPE + "' and '" + PARAM_TYPEATTR + "'.");
      }

      params.put(PARAM_TYPEATTR, map.get(PARAM_TYPEATTR).toString());
    }

    if (map.containsKey(PARAM_EXTRA)) {
      // Check that we are not using a MetaSet
      if (params.containsKey(PARAM_METASET)) {
        throw new WarpScriptException(getName() + " Cannot specify '" + PARAM_EXTRA + "' when '" + PARAM_METASET + "' is used.");
      }

      // Check that we are not using a MetaSet
      if (params.containsKey(PARAM_GTS)) {
        throw new WarpScriptException(getName() + " Cannot specify '" + PARAM_EXTRA + "' when '" + PARAM_GTS + "' is used.");
      }

      if (!(map.get(PARAM_EXTRA) instanceof List)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_EXTRA + "'.");
      }

      Set<String> extra = new HashSet<String>();

      for (Object o: (List) map.get(PARAM_EXTRA)) {
        if (!(o instanceof String)) {
          throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_EXTRA + "'.");
        }
        extra.add(o.toString());
      }

      params.put(PARAM_EXTRA, extra);
    }

    if (map.containsKey(PARAM_WRITE_TIMESTAMP)) {
      params.put(PARAM_WRITE_TIMESTAMP, Boolean.TRUE.equals(map.get(PARAM_WRITE_TIMESTAMP)));
    }

    if (map.containsKey(PARAM_TTL)) {
      params.put(PARAM_TTL, Boolean.TRUE.equals(map.get(PARAM_TTL)));
    }

    if (Boolean.TRUE.equals(params.get(PARAM_TTL)) && Boolean.TRUE.equals(params.get(PARAM_WRITE_TIMESTAMP))) {
      throw new WarpScriptException(getName() + " Only one of '" + PARAM_WRITE_TIMESTAMP + "' and '" + PARAM_TTL + "' can be set to true.");
    }

    if (map.containsKey(PARAM_ACTIVE_AFTER)) {
      if (!(map.get(PARAM_ACTIVE_AFTER) instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_ACTIVE_AFTER + "'.");
      }
      params.put(PARAM_ACTIVE_AFTER, ((long) map.get(PARAM_ACTIVE_AFTER)) / Constants.TIME_UNITS_PER_MS);
    }

    if (map.containsKey(PARAM_QUIET_AFTER)) {
      if (!(map.get(PARAM_QUIET_AFTER) instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_QUIET_AFTER + "'.");
      }
      params.put(PARAM_QUIET_AFTER, ((long) map.get(PARAM_QUIET_AFTER)) / Constants.TIME_UNITS_PER_MS);
    }

    if (map.containsKey(PARAM_SHOWUUID)) {
      params.put(PARAM_SHOWUUID, map.get(PARAM_SHOWUUID));
    }

    if (map.containsKey(PARAM_BOUNDARY)) {
      Object o = map.get(PARAM_BOUNDARY);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_BOUNDARY + "'.");
      }
      long boundary = ((Long) o).longValue();
      params.put(PARAM_BOUNDARY_PRE, boundary);
      params.put(PARAM_BOUNDARY_POST, boundary);
    }

    if (map.containsKey(PARAM_BOUNDARY_PRE)) {
      Object o = map.get(PARAM_BOUNDARY_PRE);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_BOUNDARY_PRE + "'.");
      }
      long boundary = ((Long) o).longValue();
      params.put(PARAM_BOUNDARY_PRE, boundary);
    }

    if (map.containsKey(PARAM_BOUNDARY_POST)) {
      Object o = map.get(PARAM_BOUNDARY_POST);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_BOUNDARY_POST + "'.");
      }
      long boundary = ((Long) o).longValue();
      params.put(PARAM_BOUNDARY_POST, boundary);
    }

    if (map.containsKey(PARAM_SKIP)) {
      Object o = map.get(PARAM_SKIP);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_SKIP + "'.");
      }
      long skip = ((Long) o).longValue();

      if (skip < 0) {
        throw new WarpScriptException(getName() + " Parameter '" + PARAM_SKIP + "' must be >= 0.");
      }
      params.put(PARAM_SKIP, skip);
    }

    if (map.containsKey(PARAM_STEP)) {
      Object o = map.get(PARAM_STEP);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_STEP + "'.");
      }
      long step = ((Long) o).longValue();

      if (step < 1L) {
        throw new WarpScriptException(getName() + " Parameter '" + PARAM_STEP + "' must be >= 1.");
      }
      params.put(PARAM_STEP, step);
    }

    if (map.containsKey(PARAM_TIMESTEP)) {
      Object o = map.get(PARAM_TIMESTEP);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_TIMESTEP + "'.");
      }
      long timestep = ((Long) o).longValue();

      if (timestep < 1L) {
        throw new WarpScriptException(getName() + " Parameter '" + PARAM_TIMESTEP + "' must be >= 1.");
      }
      params.put(PARAM_TIMESTEP, timestep);
    }

    if (map.containsKey(PARAM_SAMPLE)) {
      Object o = map.get(PARAM_SAMPLE);
      if (!(o instanceof Double)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_SAMPLE + "'.");
      }
      double sample = ((Double) o).doubleValue();
      if (sample <= 0.0D || sample > 1.0D) {
        throw new WarpScriptException(getName() + " Parameter '" + PARAM_SAMPLE + "' must be in the range ( 0.0, 1.0 ].");
      }

      params.put(PARAM_SAMPLE, sample);
    }

    if (map.containsKey(PARAM_LABELS_PRIORITY)) {
      Object o = map.get(PARAM_LABELS_PRIORITY);
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_LABELS_PRIORITY + "', expected a LIST.");
      }
      List<String> prio = new ArrayList<String>();
      for (Object oo: (List<Object>) o) {
        prio.add(String.valueOf(oo));
      }
      params.put(PARAM_LABELS_PRIORITY, prio);
    }

    if (map.containsKey(PARAM_ENCODERS)) {
      params.put(PARAM_ENCODERS, Boolean.TRUE.equals(map.get(PARAM_ENCODERS)));
    }

    if (map.containsKey(PARAM_MERGE)) {
      params.put(PARAM_MERGE, Boolean.TRUE.equals(map.get(PARAM_MERGE)));
    }

    if (map.containsKey(PARAM_GSKIP)) {
      Object o = map.get(PARAM_GSKIP);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_GSKIP + "'.");
      }
      long gskip = ((Long) o).longValue();

      if (gskip < 0L) {
        throw new WarpScriptException(getName() + " Parameter '" + PARAM_GSKIP + "' must be >= 0.");
      }
      params.put(PARAM_GSKIP, gskip);
    }

    if (map.containsKey(PARAM_GCOUNT)) {
      Object o = map.get(PARAM_GCOUNT);
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " Invalid type for parameter '" + PARAM_GCOUNT + "'.");
      }
      long gcount = ((Long) o).longValue();

      if (gcount < 0L) {
        throw new WarpScriptException(getName() + " Parameter '" + PARAM_GCOUNT + "' must be >= 0.");
      }
      params.put(PARAM_GCOUNT, gcount);
    }

    if (map.containsKey(PARAM_KEEPEMPTY)) {
      params.put(PARAM_KEEPEMPTY, Boolean.TRUE.equals(map.get(PARAM_KEEPEMPTY)));
    }

    return params;
  }

  /**
   * Compute the time range given start, end, timespan and count. One of these 3 parameters must be null.
   * @param start A Long, a String representing a Long, a String representing an ISO8601 date or "now". This represents the start of the time range.
   * @param startParamName The name of the start parameter, for error generation.
   * @param end A Long, a String representing a Long, a String representing an ISO8601 date or "now". This represents the end of the time range.
   * @param endParamName The name of the end parameter, for error generation.
   * @param timespan A Long, a String representing a Long or a String representing an ISO8601 duration. This represents the duration of the time range.
   * @param timespanParamName The name of the timespan parameter, for error generation.
   * @param count A positive Long. This represents the number of point to return in the time range.
   * @param countParamName The name of the count parameter, for error generation.
   * @return An array of three Longs: [start, end, timespan] with the guarantee that start and end are not null.
   * @throws WarpScriptException If the time range specification is invalid.
   */
  public static Long[] computeTimeRange(Object start, String startParamName, Object end, String endParamName, Object timespan, String timespanParamName, Object count, String countParamName) throws WarpScriptException {
    long now = TimeSource.getTime();

    //
    // Try to convert start to a valid timestamp if possible.
    //

    Long startTs = getTimestamp(start, startParamName, now);

    //
    // Try to convert end to a valid timestamp if possible.
    // Same exact logic as for start.
    //

    Long endTs = getTimestamp(end, endParamName, now);

    // Check that either startTs or endTs is defined.
    if (null == startTs && null == endTs) {
      throw new WarpScriptException("Missing either '" + startParamName + "' or '" + endParamName + "' parameter.");
    }

    // If both are defined but swapped, swap them.
    if (null != startTs && null != endTs && startTs > endTs) {
      long tmp = startTs;
      startTs = endTs;
      endTs = tmp;
    }

    //
    // Try to use timestamp to either determine endTs or startTs if not already defined.
    //

    Long numericTimespan = null;

    if (null != timespan) {
      // Check that endTs and startTs are not both already defined.
      if (startTs != null && endTs != null) {
        throw new WarpScriptException("Invalid time range specification: '" + startParamName + "', '" + endParamName + "' and '" + timespanParamName + "' cannot all be defined. Only 2 out of those 3 parameters should be defined.");
      }

      //
      // Cast or convert timespan to a numeric one.
      //
      if (timespan instanceof Long) {
        numericTimespan = (Long) timespan;
      } else if (timespan instanceof String) {
        // If it's a string, it may be the string representation of a Long or a ISO8601 duration.
        if (0 != ((String) timespan).length()) {
          try {
            // Speed up choice between ISO8601 and Long by checking the first character instead of relying on exceptions.
            if ('P' == ((String) timespan).charAt(0)) {
              // Should be a ISO8601 duration
              ReadWritablePeriod period = new MutablePeriod();

              ISOPeriodFormat.standard().getParser().parseInto(period, (String) timespan, 0, Locale.US);

              Period p = period.toPeriod();

              // TODO(tce) This could be removed if we add this period to start or subtract to end. However we need
              // to keep track of the timezone which require quite a lot of change in the code.
              if (p.getMonths() != 0 || p.getYears() != 0) {
                throw new WarpScriptException("No support for ambiguous durations containing years or months, please convert those to days.");
              }

              Duration duration = p.toDurationFrom(new Instant());

              numericTimespan = duration.getMillis() * Constants.TIME_UNITS_PER_MS;
            } else {
              // Should be a Long representation
              numericTimespan = Long.parseLong((String) timespan);
            }
          } catch (IllegalArgumentException | WarpScriptException e) {
            throw new WarpScriptException("Invalid format for parameter '" + timespanParamName + "'.", e);
          }
        } else {
          throw new WarpScriptException("Parameter '" + timespanParamName + "' is empty.");
        }
      } else {
        // If timespan is not null and not a Long nor a String, throw an error.
        throw new WarpScriptException("Invalid format for parameter '" + timespanParamName + "'.");
      }

      if (numericTimespan < 0) {
        throw new WarpScriptException("'" + timespanParamName + "' cannot be negative.");
      }

      if (null == startTs) {
        // In that case startTs is not defined, so we compute it.

        // Check edge case
        if (0L == numericTimespan && Long.MAX_VALUE == endTs) {
          throw new WarpScriptException("Cannot set '" + timespanParamName + "' to 0 and '" + endParamName + "' to MAX_VALUE.");
        }

        try {
          // No need to check for overflow for '+ 1' on the line below because this edge case has already been checked.
          startTs = Math.subtractExact(endTs, numericTimespan) + 1;
        } catch (ArithmeticException ae) {
          startTs = Long.MIN_VALUE;
        }
      } else { // endTs == null
        // In that case endTs is not defined, so we compute it.

        // Check edge case
        if (0L == numericTimespan && Long.MIN_VALUE == startTs) {
          throw new WarpScriptException("Cannot set '" + timespanParamName + "' to 0 and '" + startParamName + "' to MIN_VALUE.");
        }

        try {
          // No need to check for overflow for '- 1' on the line below because this edge case has already been checked.
          endTs = Math.addExact(startTs, numericTimespan) - 1;
        } catch (ArithmeticException ae) {
          endTs = Long.MAX_VALUE;
        }
      }
    }

    // Check that at least endTs is defined.
    if (null == endTs) {
      throw new WarpScriptException("Missing '" + endParamName + "' or '" + startParamName + "' and '" + timespanParamName + "' parameter.");
    }

    // Make sure startTs is defined.
    if (null == startTs) {
      if (null == count) {
        throw new WarpScriptException("Invalid time range specification: '" + countParamName + "' is mandatory if '" + startParamName + "' and '" + timespanParamName + "' are not specified.");
      } else {
        // Fetch with end and count: start is set to the beginnings of time.
        startTs = Long.MIN_VALUE;
      }
    }

    return new Long[] {startTs, endTs, numericTimespan};
  }

  public static Long getTimestamp(Object timestampRepresentation, String timestampRepresentationParameterName, Long nowTimestamp) throws WarpScriptException {
    Long timestamp = null;

    if (timestampRepresentation instanceof Long) {
      // Simple case: start is a long
      timestamp = (long) timestampRepresentation;
    } else if (timestampRepresentation instanceof String) {
      // If it's a string, it may be the string representation of a Long, a ISO8601 date or 'now'.
      if (NOW_PARAM_VALUE.equals(timestampRepresentation)) {
        timestamp = nowTimestamp;
      } else {
        try {
          timestamp = Long.parseLong((String) timestampRepresentation);
        } catch (NumberFormatException nfe) {
          // Not string representation of a Long, try ISO8601
          try {
            timestamp = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp((String) timestampRepresentation);
          } catch (WarpScriptException | IllegalArgumentException e) {
            // Don't set the cause of the execption because we don't know which of the two (nfs or e) it is.
            throw new WarpScriptException("Invalid format for parameter '" + timestampRepresentationParameterName + "'.");
          }
        }
      }
    } else if (null != timestampRepresentation) {
      // If start is not null and we cannot retrieve the timestamp, throw an error.
      throw new WarpScriptException("Invalid format for parameter '" + timestampRepresentationParameterName + "'.");
    }

    return timestamp;
  }

  public static Map<String,Object> parseTokenAttributes(ReadToken token, Map<String,Object> params) throws WarpScriptException {
    Map<String,Object> attributes = new HashMap<String,Object>();

    Map<String,String> tokenattr = token.getAttributes();

    if (null == tokenattr) {
      return attributes;
    }

    long now = TimeSource.getTime();

    Instant ref = new Instant(now / Constants.TIME_UNITS_PER_MS);

    String value = tokenattr.get(Constants.TOKEN_ATTR_END);

    Long end = null;

    if (null != value) {
      value = value.trim();

      if (value.startsWith("P")) {
        long offset = DURATION.parseDuration(ref, value, true, true);
        end = now - offset;
      } else {
        end = TOTIMESTAMP.parseTimestamp(value);
      }
      attributes.put(PARAM_END, end);
    }

    value = tokenattr.get(Constants.TOKEN_ATTR_START);
    if (null != value) {
      value = value.trim();
      long start;

      if (value.startsWith("P")) {
        if (null == end) {
          if (null != params && params.get(PARAM_END) instanceof Long) {
            long t = ((Long) params.get(PARAM_END)).longValue();
            long offset = DURATION.parseDuration(new Instant(t / Constants.TIME_UNITS_PER_MS), value, true, true);
            start = t - offset;
          } else {
            long offset = DURATION.parseDuration(ref, value, true, true);
            start = now - offset;
          }
        } else {
          long offset = DURATION.parseDuration(new Instant(end / Constants.TIME_UNITS_PER_MS), value, true, true);
          start = end - offset;
        }
      } else {
        start = TOTIMESTAMP.parseTimestamp(value);
      }
      attributes.put(PARAM_START, start);
    }

    value = tokenattr.get(Constants.TOKEN_ATTR_QUIETAFTER);
    if (null != value) {
      value = value.trim();
      long quietafter;

      if (value.startsWith("P")) {
        if (null == end) {
          if (null != params && params.get(PARAM_END) instanceof Long) {
            long t = ((Long) params.get(PARAM_END)).longValue();
            long offset = DURATION.parseDuration(new Instant(t / Constants.TIME_UNITS_PER_MS), value, true, true);
            quietafter = t - offset;
          } else {
            long offset = DURATION.parseDuration(ref, value, true, true);
            quietafter = now - offset;
          }
        } else {
          long offset = DURATION.parseDuration(new Instant(end / Constants.TIME_UNITS_PER_MS), value, true, true);
          quietafter = end - offset;
        }
      } else {
        quietafter = TOTIMESTAMP.parseTimestamp(value);
      }
      attributes.put(PARAM_QUIET_AFTER, quietafter);
    }

    value = tokenattr.get(Constants.TOKEN_ATTR_ACTIVEAFTER);
    if (null != value) {
      value = value.trim();
      Long activeafter = null;

      if (value.startsWith("P")) {
        if (null == end) {
          if (null != params && params.get(PARAM_END) instanceof Long) {
            long t = ((Long) params.get(PARAM_END)).longValue();
            long offset = DURATION.parseDuration(new Instant(t / Constants.TIME_UNITS_PER_MS), value, true, true);
            activeafter = t - offset;
          } else {
            long offset = DURATION.parseDuration(ref, value, true, true);
            activeafter = now - offset;
          }
        } else {
          long offset = DURATION.parseDuration(new Instant(end / Constants.TIME_UNITS_PER_MS), value, true, true);
          activeafter = end - offset;
        }
      } else if ("0".equals(value) && params.get(PARAM_START) instanceof Long) {
        activeafter = ((Long) params.get(PARAM_START)).longValue();
      } else {
        activeafter = TOTIMESTAMP.parseTimestamp(value);
      }
      attributes.put(PARAM_ACTIVE_AFTER, activeafter);
    }

    value = tokenattr.get(Constants.TOKEN_ATTR_COUNT);
    if (null != value) {
      value = value.trim();
      long count;

      try {
        count = Long.parseLong(value);

        if (count < 0) {
          throw new WarpScriptException("Invalid count specification.");
        }
      } catch (NumberFormatException nfe) {
        throw new WarpScriptException("Invalid count specification.");
      }
      attributes.put(PARAM_COUNT, count);
    }

    value = tokenattr.get(Constants.TOKEN_ATTR_BOUNDARY_PRE);
    if (null != value) {
      value = value.trim();
      long preboundary;

      try {
        preboundary = Long.parseLong(value);

        if (preboundary < 0) {
          throw new WarpScriptException("Invalid pre boundary specification.");
        }
      } catch (NumberFormatException nfe) {
        throw new WarpScriptException("Invalid pre boundary specification.");
      }
      attributes.put(PARAM_BOUNDARY_PRE, preboundary);
    }

    value = tokenattr.get(Constants.TOKEN_ATTR_BOUNDARY_POST);
    if (null != value) {
      value = value.trim();
      long postboundary;

      try {
        postboundary = Long.parseLong(value);

        if (postboundary < 0) {
          throw new WarpScriptException("Invalid post boundary specification.");
        }
      } catch (NumberFormatException nfe) {
        throw new WarpScriptException("Invalid post boundary specification.");
      }
      attributes.put(PARAM_BOUNDARY_POST, postboundary);
    }

    return attributes;
  }
}
