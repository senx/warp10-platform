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

package io.warp10.continuum.egress;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.GTSSplit;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.warp10.sensision.Sensision;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;
import org.bouncycastle.util.encoders.Hex;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.GeoXPLib;
import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

public class EgressFetchHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EgressFetchHandler.class);
  
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  private final StoreClient storeClient;
    
  private final DirectoryClient directoryClient;
  
  private final byte[] fetchPSK;
  
  private final byte[] fetchAES;

  private final long maxSplitAge;
  
  public static final Pattern SELECTOR_RE = Pattern.compile("^([^{]+)\\{(.*)\\}$");

  /**
   * Maximum number of GTS per call to the fetch endpoint
   */
  public static long FETCH_BATCHSIZE = 100000;
  
  public EgressFetchHandler(KeyStore keystore, Properties properties, DirectoryClient directoryClient, StoreClient storeClient) {
    this.fetchPSK = keystore.getKey(KeyStore.SIPHASH_FETCH_PSK);
    this.fetchAES = keystore.getKey(KeyStore.AES_FETCHER);
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    
    if (properties.containsKey(Configuration.EGRESS_FETCH_BATCHSIZE)) {
      FETCH_BATCHSIZE = Long.parseLong(properties.getProperty(Configuration.EGRESS_FETCH_BATCHSIZE));
    }
    
    if (properties.containsKey(Configuration.EGRESS_FETCHER_MAXSPLITAGE)) {
      this.maxSplitAge = Long.parseLong(properties.getProperty(Configuration.EGRESS_FETCHER_MAXSPLITAGE));
    } else {
      this.maxSplitAge = Long.MAX_VALUE;
    }
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

    boolean fromArchive = false;
    boolean splitFetch = false;
    boolean writeTimestamp = false;
    
    if (Constants.API_ENDPOINT_FETCH.equals(target)) {
      baseRequest.setHandled(true);
      fromArchive = false;
    } else if (Constants.API_ENDPOINT_AFETCH.equals(target)) {
      baseRequest.setHandled(true);
      fromArchive = true;
    } else if (Constants.API_ENDPOINT_SFETCH.equals(target)) {
      baseRequest.setHandled(true);
      splitFetch = true;
    } else {
      return;
    }

    // Labels for Sensision
    Map<String,String> labels = new HashMap<String,String>();

    labels.put(SensisionConstants.SENSISION_LABEL_TYPE, target);

    //
    // Add CORS header
    //
    
    resp.setHeader("Access-Control-Allow-Origin", "*");

    String start = null;
    String stop = null;
    
    long now = Long.MIN_VALUE;
    long timespan = 0L;

    String nowParam = null;
    String timespanParam = null;
    String dedupParam = null;

    if (splitFetch) {
      nowParam = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX));
      timespanParam = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TIMESPAN_HEADERX));
    } else {
      start = req.getParameter(Constants.HTTP_PARAM_START);
      stop = req.getParameter(Constants.HTTP_PARAM_STOP);
      
      nowParam = req.getParameter(Constants.HTTP_PARAM_NOW);
      timespanParam = req.getParameter(Constants.HTTP_PARAM_TIMESPAN);
      dedupParam = req.getParameter(Constants.HTTP_PARAM_DEDUP);      
    }
        
    boolean dedup = null != dedupParam && "true".equals(dedupParam);
    
    if (null != start && null != stop) {
      long tsstart = fmt.parseDateTime(start).getMillis() * Constants.TIME_UNITS_PER_MS;
      long tsstop = fmt.parseDateTime(stop).getMillis() * Constants.TIME_UNITS_PER_MS;
      
      if (tsstart < tsstop) {
        now = tsstop;
        timespan = tsstop - tsstart;
      } else {
        now = tsstart;
        timespan = tsstart - tsstop;
      }
    } else if (null != nowParam && null != timespanParam) {
      try {
        now = Long.parseLong(nowParam);
      } catch (Exception e) {
        now = fmt.parseDateTime(nowParam).getMillis() * Constants.TIME_UNITS_PER_MS;
      }
      
      timespan = Long.parseLong(timespanParam);
    }
    
    if (Long.MIN_VALUE == now) {
      resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing now/timespan or start/stop parameters.");
      return;
    }
      
    String selector = splitFetch ? null : req.getParameter(Constants.HTTP_PARAM_SELECTOR);
 
    //
    // Extract token from header
    //
    
    String token = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
    
    // If token was not found in header, extract it from the 'token' parameter
    if (null == token && !splitFetch) {
      token = req.getParameter(Constants.HTTP_PARAM_TOKEN);
    }
    
    String fetchSig = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_FETCH_SIGNATURE));

    //
    // Check token signature if it was provided
    //
    
    boolean signed = false;
    
    if (splitFetch) {
      signed = true;
    }
    
    if (null != fetchSig) {
      if (null != fetchPSK) {
        String[] subelts = fetchSig.split(":");
        if (2 != subelts.length) {
          throw new IOException("Invalid fetch signature.");
        }
        long nowts = System.currentTimeMillis();
        long sigts = new BigInteger(subelts[0], 16).longValue();
        long sighash = new BigInteger(subelts[1], 16).longValue();
        
        if (nowts - sigts > 10000L) {
          throw new IOException("Fetch signature has expired.");
        }
        
        // Recompute hash of ts:token
        
        String tstoken = Long.toString(sigts) + ":" + token;
        
        long checkedhash = SipHashInline.hash24(fetchPSK, tstoken.getBytes(Charsets.ISO_8859_1));
        
        if (checkedhash != sighash) {
          throw new IOException("Corrupted fetch signature");
        }
    
        signed = true;
      } else {
        throw new IOException("Fetch PreSharedKey is not set.");
      }
    }
    
    ReadToken rtoken = null;
    
    String format = splitFetch ? "wrapper" : req.getParameter(Constants.HTTP_PARAM_FORMAT);

    if (!splitFetch) {
      try {
        rtoken = Tokens.extractReadToken(token);
        
        if (rtoken.getHooksSize() > 0) {
          throw new IOException("Tokens with hooks cannot be used for fetching data.");        
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }
            
      if (null == rtoken) {
        resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing token.");
        return;
      }      
    }
    
    //
    // Extract the class and labels selectors
    // The class selector and label selectors are supposed to have
    // values which use percent encoding, i.e. explicit percent encoding which
    // might have been re-encoded using percent encoding when passed as parameter
    //
    //
    
    Set<Metadata> metadatas = new HashSet<Metadata>();
    List<Iterator<Metadata>> iterators = new ArrayList<Iterator<Metadata>>();
    
    if (!splitFetch) {
      String[] selectors = selector.split("\\s+");
      
      for (String sel: selectors) {
        Matcher m = SELECTOR_RE.matcher(sel);
        
        if (!m.matches()) {
          resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
          return;
        }
        
        String classSelector = URLDecoder.decode(m.group(1), "UTF-8");
        String labelsSelection = m.group(2);
        
        Map<String,String> labelsSelectors;

        try {
          labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
        } catch (ParseException pe) {
          throw new IOException(pe);
        }
        
        //
        // Force 'producer'/'owner'/'app' from token
        //
        
        labelsSelectors.remove(Constants.PRODUCER_LABEL);
        labelsSelectors.remove(Constants.OWNER_LABEL);
        labelsSelectors.remove(Constants.APPLICATION_LABEL);
        
        labelsSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

        List<Metadata> metas = null;
        
        List<String> clsSels = new ArrayList<String>();
        List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
        
        clsSels.add(classSelector);
        lblsSels.add(labelsSelectors);
        
        try {
          metas = directoryClient.find(clsSels, lblsSels);
          metadatas.addAll(metas);
        } catch (Exception e) {
          //
          // If metadatas is not empty, create an iterator for it, then clear it
          //
          if (!metadatas.isEmpty()) {
            iterators.add(metadatas.iterator());
            metadatas.clear();
          }
          iterators.add(directoryClient.iterator(clsSels, lblsSels));
        }
      }      
    } else {
      //
      // Add an iterator which reads splits from the request body
      //
      
      boolean gzipped = false;
      
      if (null != req.getHeader("Content-Type") && "application/gzip".equals(req.getHeader("Content-Type"))) {
        gzipped = true;
      }
      
      BufferedReader br = null;
          
      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(req.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {    
        br = req.getReader();
      }

      final BufferedReader fbr = br;
      

      MetadataIterator iterator = new MetadataIterator() {
        
        private List<Metadata> metadatas = new ArrayList<Metadata>();
        
        private boolean done = false;

        private String lasttoken = "";
        
        @Override
        public void close() throws Exception {
          fbr.close();
        }
        
        @Override
        public Metadata next() {
          if (!metadatas.isEmpty()) {
            Metadata meta = metadatas.get(metadatas.size() - 1);
            metadatas.remove(metadatas.size() - 1);
            return meta;
          } else {
            if (hasNext()) {
              return next();
            } else {
              throw new NoSuchElementException();
            }
          }
        }
        
        @Override
        public boolean hasNext() {
          if (!metadatas.isEmpty()) {
            return true;
          }
          
          if (done) {
            return false;            
          }
          
          String line = null;
          
          try {
            line = fbr.readLine();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          
          if (null == line) {
            done = true;
            return false;
          }
          
          //
          // Decode/Unwrap/Deserialize the split
          //
          
          byte[] data = OrderPreservingBase64.decode(line.getBytes(Charsets.US_ASCII));
          if (null != fetchAES) {
            data = CryptoUtils.unwrap(fetchAES, data);
          }
          
          if (null == data) {
            throw new RuntimeException("Invalid wrapped content.");
          }
          
          TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
          
          GTSSplit split = new GTSSplit();
          
          try {
            deserializer.deserialize(split, data);
          } catch (TException te) {
            throw new RuntimeException(te);
          }
          
          //
          // Check the expiry
          //
          
          long instant = System.currentTimeMillis();
          
          if (instant - split.getTimestamp() > maxSplitAge || instant > split.getExpiry()) {
            throw new RuntimeException("Split has expired.");
          }
                    
          this.metadatas.addAll(split.getMetadatas());
          
          // We assume there was at least one metadata instance in the split!!!
          return true;
        }
      };
      
      iterators.add(iterator);
    }
       
    List<Metadata> metas = new ArrayList<Metadata>();
    metas.addAll(metadatas);
    
    if (!metas.isEmpty()) {
      iterators.add(metas.iterator());
    }
    
    //
    // Loop over the iterators, storing the read metadata to a temporary file encrypted on disk
    // Data is encrypted using a onetime pad
    //
    
    final byte[] onetimepad = new byte[(int) Math.min(65537, System.currentTimeMillis() % 100000)];
    new Random().nextBytes(onetimepad);
    
    final File cache = File.createTempFile(Long.toHexString(System.currentTimeMillis()) + "-" + Long.toHexString(System.nanoTime()), ".dircache");
    cache.deleteOnExit();
    
    FileWriter writer = new FileWriter(cache);

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    int padidx = 0;
    
    for (Iterator<Metadata> itermeta: iterators){
      try {
        while(itermeta.hasNext()) {
          Metadata metadata = itermeta.next();
          
          try {
            byte[] bytes = serializer.serialize(metadata);
            // Apply onetimepad
            for (int i = 0; i < bytes.length; i++) {
              bytes[i] = (byte) (bytes[i] ^ onetimepad[padidx++]);
              if (padidx >= onetimepad.length) {
                padidx = 0;
              }
            }
            bytes = OrderPreservingBase64.encode(bytes);
            writer.write(new String(bytes, Charsets.US_ASCII));
            writer.write('\n');
          } catch (TException te) {          
          }         
        }
        
        if (!itermeta.hasNext() && (itermeta instanceof MetadataIterator)) {
          try {
            ((MetadataIterator) itermeta).close();
          } catch (Exception e) {          
          }
        }              
      } catch (Throwable t) {
        throw t;
      } finally {
        if (itermeta instanceof MetadataIterator) {
          try {
            ((MetadataIterator) itermeta).close();
          } catch (Exception e) {        
          }
        }
      }
    }
    
    writer.close();
    
    //
    // Create an iterator based on the cache
    //
    
    MetadataIterator cacheiterator = new MetadataIterator() {
      
      BufferedReader reader = new BufferedReader(new FileReader(cache));
      
      private Metadata current = null;
      private boolean done = false;
      
      private TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      
      int padidx = 0;
      
      @Override
      public boolean hasNext() {
        if (done) {
          return false;
        }
        
        if (null != current) {
          return true;
        }
        
        try {
          String line = reader.readLine();
          if (null == line) {
            done = true;
            return false;
          }
          byte[] raw = OrderPreservingBase64.decode(line.getBytes(Charsets.US_ASCII));
          // Apply one time pad
          for (int i = 0; i < raw.length; i++) {
            raw[i] = (byte) (raw[i] ^ onetimepad[padidx++]);
            if (padidx >= onetimepad.length) {
              padidx = 0;
            }
          }
          Metadata metadata = new Metadata();
          try {
            deserializer.deserialize(metadata, raw);
            this.current = metadata;
            return true;
          } catch (TException te) {
            LOG.error("", te);
          }
        } catch (IOException ioe) {
          LOG.error("", ioe);
        }
        
        return false;
      }
      
      @Override
      public Metadata next() {
        if (null != this.current) {
          Metadata metadata = this.current;
          this.current = null;
          return metadata;
        } else {
          throw new NoSuchElementException();
        }
      }
      
      @Override
      public void close() throws Exception {
        this.reader.close();
        cache.delete();
      }
    };
    
    iterators.clear();
    iterators.add(cacheiterator);
        
    metas = new ArrayList<Metadata>();
    
    for (Iterator<Metadata> itermeta: iterators) {
      while(itermeta.hasNext()) {
        metas.add(itermeta.next());
        
        //
        // Access the data store every 'FETCH_BATCHSIZE' GTS or at the end of each iterator
        //
        
        if (metas.size() > FETCH_BATCHSIZE || !itermeta.hasNext()) {
          try(GTSDecoderIterator iter = storeClient.fetch(rtoken, metas, now, timespan, fromArchive, writeTimestamp)) {
            if("text".equals(format)) {
              textDump(resp, iter, now, timespan, false, dedup, signed);
            } else if ("fulltext".equals(format)) {
              textDump(resp, iter, now, timespan, true, dedup, signed);
            } else if ("raw".equals(format)) {
              rawDump(resp, iter, dedup, signed);
            } else if ("wrapper".equals(format)) {
              wrapperDump(resp, iter, dedup, signed, fetchPSK);
            } else if ("json".equals(format)) {
              jsonDump(resp, iter, now, timespan, dedup, signed);
            } else if ("tsv".equals(format)) {
              tsvDump(resp, iter, now, timespan, false, dedup, signed);
            } else if ("fulltsv".equals(format)) {
              tsvDump(resp, iter, now, timespan, true, dedup, signed);
            } else {
              textDump(resp, iter, now, timespan, false, dedup, signed);
            }
          } catch (Exception e) {
            LOG.error("",e);
            throw new IOException(e);
          } finally {      
            if (!itermeta.hasNext() && (itermeta instanceof MetadataIterator)) {
              try {
                ((MetadataIterator) itermeta).close();
              } catch (Exception e) {          
              }
            }
          }                  
          
          //
          // Reset 'metas'
          //
          
          metas.clear();
        }        
      }
      
      if (!itermeta.hasNext() && (itermeta instanceof MetadataIterator)) {
        try {
          ((MetadataIterator) itermeta).close();
        } catch (Exception e) {          
        }
      }
    }

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_REQUESTS, labels, 1);

  }
  
  private static void rawDump(HttpServletResponse resp, GTSDecoderIterator iter, boolean dedup, boolean signed) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder sb = new StringBuilder();
    
    OutputStream out = resp.getOutputStream();

    PrintWriter pw = new PrintWriter(out);
    //PrintWriter pw = resp.getWriter();

    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();

      if (dedup) {
        decoder = decoder.dedup();
      }
      
      if(!decoder.next()) {
        continue;
      }

      GTSEncoder encoder = decoder.getEncoder(true);
      
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the name
      //

      name = decoder.getName();
      labels = lbls;
      sb.setLength(0);
      GTSHelper.encodeName(sb, name);
      sb.append("{");
      boolean first = true;
      
      for (Entry<String, String> entry: lbls.entrySet()) {
        //
        // Skip owner/producer labels and any other 'private' labels
        //
        if (!signed) {
          if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
            continue;
          }
          if (Constants.OWNER_LABEL.equals(entry.getKey())) {
            continue;
          }          
        }
        
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");

      pw.print(encoder.getBaseTimestamp());
      pw.print("//");
      pw.print(encoder.getCount());
      pw.print(" ");
      pw.print(sb.toString());
      pw.print(" ");
      pw.flush();

      //pw.println(new String(OrderPreservingBase64.encode(encoder.getBytes())));
      
      OrderPreservingBase64.encodeToStream(encoder.getBytes(), out);
      out.write('\r');
      out.write('\n');
      out.flush();
      
    }        
  }

  private static void wrapperDump(HttpServletResponse resp, GTSDecoderIterator iter, boolean dedup, boolean signed, byte[] fetchPSK) throws IOException {

    if (!signed) {
      throw new IOException("Unsigned request.");
    }

    // Labels for Sensision
    Map<String,String> labels = new HashMap<String,String>();

    StringBuilder sb = new StringBuilder();
    
    OutputStream out = resp.getOutputStream();

    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();

      if (dedup) {
        decoder = decoder.dedup();
      }
      
      if(!decoder.next()) {
        continue;
      }

      GTSEncoder encoder = decoder.getEncoder(true);

      //
      // Build a GTSWrapper
      //
      
      GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
      
//      GTSWrapper wrapper = new GTSWrapper();
//      wrapper.setBase(encoder.getBaseTimestamp());
//      wrapper.setMetadata(encoder.getMetadata());
//      wrapper.setCount(encoder.getCount());
//      wrapper.setEncoded(encoder.getBytes());
      
      //
      // Serialize the wrapper
      //
      
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      byte[] data = null;
      
      try {
        data = serializer.serialize(wrapper);
      } catch (TException te) {
        throw new IOException(te);
      }
      
      //
      // Output is GTSWrapperId <WSP> HASH <WSP> GTSWrapper
      //
      
      out.write(Hex.encode(GTSWrapperHelper.getId(wrapper)));
      
      out.write(' ');
      
      if (null != fetchPSK) {
        //
        // Compute HMac for the wrapper
        //
        
        long hash = SipHashInline.hash24(fetchPSK, data);
        
        //
        // Output the MAC before the data, as hex digits
        //

        out.write(Hex.encode(Longs.toByteArray(hash)));               
      } else {
        out.write('-');
      }
      
      out.write(' ');
      
      //
      // Base64 encode the wrapper
      //
      
      OrderPreservingBase64.encodeToStream(data, out);
      out.write('\r');
      out.write('\n');
      out.flush();

      //
      // Sensision metrics
      //

      labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, wrapper.getMetadata().getLabels().get(Constants.APPLICATION_LABEL));

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS, Sensision.EMPTY_LABELS, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_PERAPP, labels, 1);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPER_SIZE, Sensision.EMPTY_LABELS, data.length);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPER_SIZE_PERAPP, labels, data.length);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPER_DATAPOINTS, Sensision.EMPTY_LABELS, wrapper.getCount());
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPER_DATAPOINTS_PERAPP, labels, wrapper.getCount());

    }        
  }
  
  /**
   * Output a text version of fetched data. Deduplication is done on the fly so we don't decode twice.
   * 
   */
  private static void textDump(HttpServletResponse resp, GTSDecoderIterator iter, long now, long timespan, boolean raw, boolean dedup, boolean signed) throws IOException {
    
    //resp.setContentType("text/plain");
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder sb = new StringBuilder();
    
    PrintWriter pw = resp.getWriter();
    
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();
      
      if (!decoder.next()) {
        continue;
      }
    
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the new name
      //

      boolean displayName = false;
      
      if (null == name || (!name.equals(decoder.getName()) || !labels.equals(lbls))) {
        displayName = true;
        name = decoder.getName();
        labels = lbls;
        sb.setLength(0);
        GTSHelper.encodeName(sb, name);
        sb.append("{");
        boolean first = true;
        
        for (Entry<String, String> entry: lbls.entrySet()) {
          //
          // Skip owner/producer labels and any other 'private' labels
          //
          if (!signed) {
            if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
              continue;
            }
            if (Constants.OWNER_LABEL.equals(entry.getKey())) {
              continue;
            }            
          }
          
          if (!first) {
            sb.append(",");
          }
          GTSHelper.encodeName(sb, entry.getKey());
          sb.append("=");
          GTSHelper.encodeName(sb, entry.getValue());
          first = false;
        }
        sb.append("}");
      }
      
      long timestamp = 0L;
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;
      Object value = null;
      
      boolean dup = true;
      
      do {
        // FIXME(hbs): only display the results which match the authorized (according to token) timerange and geo zones
  
        //
        // Filter out any value not in the time range
        //
        
        long newTimestamp = decoder.getTimestamp();
        
        if (newTimestamp > now || (timespan >= 0 && newTimestamp <= (now -timespan))) {
          continue;
        }
        
        //
        // TODO(hbs): filter out values with no location or outside the selected geozone when a geozone was set
        //
        
        long newLocation = decoder.getLocation();
        long newElevation = decoder.getElevation();
        Object newValue = decoder.getValue();
        
        dup = true;
        
        if (dedup) {
          if (location != newLocation || elevation != newElevation) {
            dup = false;
          } else {
            if (null == newValue) {
              // Consider nulls as duplicates (can't happen!)
              dup = false;
            } else if (newValue instanceof Number) {
              if (!((Number) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof String) {
              if (!((String) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof Boolean) {
              if (!((Boolean) newValue).equals(value)) {
                dup = false;
              }
            }
          }          
        }
                
        location = newLocation;
        elevation = newElevation;
        timestamp = newTimestamp;
        value = newValue;
            
        if (raw) {
          if (!dedup || !dup) {
            pw.println(GTSHelper.tickToString(sb, timestamp, location, elevation, value));
          }
        } else {
          // Display the name only if we have at least one value to display
          // We force 'dup' to be false when we must show the name
          if (displayName) {
            pw.println(GTSHelper.tickToString(sb, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getValue()));
            displayName = false;
            dup = false;
          } else {
            if (!dedup || !dup) {
              pw.print("=");
              pw.println(GTSHelper.tickToString(timestamp,
                  location,
                  elevation,
                  value));                                    
            }
          }
        }
      } while (decoder.next());        
      
      // Print any remaining value
      if (dedup && dup) {
        if (raw) {
          pw.println(GTSHelper.tickToString(sb, timestamp, location, elevation, value));          
        } else {
          pw.print("=");
          pw.println(GTSHelper.tickToString(timestamp,
              location,
              elevation,
              value));                                              
        }
      }
      
      //
      // If displayName is still true it means we should have displayed the name but no value matched,
      // so set name to null so we correctly display the name for the next decoder if it has values
      //
      
      if (displayName) {
        name = null;
      }
    }    
  }

  private static void jsonDump(HttpServletResponse resp, GTSDecoderIterator iter, long now, long timespan, boolean dedup, boolean signed) throws IOException {
    
    //resp.setContentType("application/json");
    
    PrintWriter pw = resp.getWriter();
    
    String name = null;
    Map<String,String> labels = null;
    
    pw.print("[");
    
    StringBuilder sb = new StringBuilder();
    
    JsonSerializer serializer = new JsonSerializerFactory().create();
    
    boolean firstgts = true;

    boolean hasValues = false;
    
    long mask = (long) (Math.random() * Long.MAX_VALUE);
    
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();
      
      if (dedup) {
        decoder = decoder.dedup();
      }
      
      if (!decoder.next()) {
        continue;
      }
          
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the new name
      //

      boolean displayName = false;
      
      if (null == name || (!name.equals(decoder.getName()) || !labels.equals(lbls))) {
        displayName = true;
        name = decoder.getName();
        labels = lbls;
        sb.setLength(0);
        
        sb.append("{\"c\":");
    
        //sb.append(gson.toJson(name));
        sb.append(serializer.serialize(name));

        boolean first = true;
        
        sb.append(",\"l\":{");
        
        for (Entry<String, String> entry: lbls.entrySet()) {
          //
          // Skip owner/producer labels and any other 'private' labels
          //
          if (!signed) {
            if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
              continue;
            }
            if (Constants.OWNER_LABEL.equals(entry.getKey())) {
              continue;
            }            
          }
          
          if (!first) {
            sb.append(",");
          }
          
          //sb.append(gson.toJson(entry.getKey()));
          sb.append(serializer.serialize(entry.getKey()));
          sb.append(":");
          //sb.append(gson.toJson(entry.getValue()));
          sb.append(serializer.serialize(entry.getValue()));
          first = false;
        }
        sb.append("}");
        
        sb.append(",\"a\":{");

        first = true;
        for (Entry<String, String> entry: decoder.getMetadata().getAttributes().entrySet()) {
          if (!first) {
            sb.append(",");
          }
          
          //sb.append(gson.toJson(entry.getKey()));
          sb.append(serializer.serialize(entry.getKey()));
          sb.append(":");
          //sb.append(gson.toJson(entry.getValue()));
          sb.append(serializer.serialize(entry.getValue()));
          first = false;
        }
        
        sb.append("}");
        sb.append(",\"i\":\"");
        sb.append(decoder.getLabelsId() & mask);
        sb.append("\",\"v\":[");
      }
      
      do {
        // FIXME(hbs): only display the results which match the authorized (according to token) timerange and geo zones
  
        //
        // Filter out any value not in the time range
        //
        
        if (decoder.getTimestamp() > now || (timespan >= 0 && decoder.getTimestamp() <= (now -timespan))) {
          continue;
        }
        
        //
        // TODO(hbs): filter out values with no location or outside the selected geozone when a geozone was set
        //
        
        // Display the name only if we have at least one value to display
        if (displayName) {
          if (!firstgts) {
            pw.print("]},");
          }
          pw.print(sb.toString());
          firstgts = false;
          displayName = false;
        } else {
          pw.print(",");
        }
        hasValues = true;
        pw.print("[");
        pw.print(decoder.getTimestamp());
        if (GeoTimeSerie.NO_LOCATION != decoder.getLocation()) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(decoder.getLocation());
          pw.print(",");
          pw.print(latlon[0]);
          pw.print(",");
          pw.print(latlon[1]);
        }
        if (GeoTimeSerie.NO_ELEVATION != decoder.getElevation()) {
          pw.print(",");
          pw.print(decoder.getElevation());
        }
        pw.print(",");
        Object value = decoder.getValue();
        
        if (value instanceof Number) {
          pw.print(value);
        } else if (value instanceof Boolean) {
          pw.print(Boolean.TRUE.equals(value) ? "true" : "false");
        } else {
          //pw.print(gson.toJson(value.toString()));
          pw.print(serializer.serialize(value.toString()));
        }
        pw.print("]");
      } while (decoder.next());        
      
      //
      // If displayName is still true it means we should have displayed the name but no value matched,
      // so set name to null so we correctly display the name for the next decoder if it has values
      //
      
      if (displayName) {
        name = null;
      }
    }
    
    if (hasValues) {
      pw.print("]}");
    }
    pw.print("]");
  }
  
  /**
   * Output a tab separated version of fetched data. Deduplication is done on the fly so we don't decode twice.
   * 
   */
  private static void tsvDump(HttpServletResponse resp, GTSDecoderIterator iter, long now, long timespan, boolean raw, boolean dedup, boolean signed) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder classSB = new StringBuilder();
    StringBuilder labelsSB = new StringBuilder();
    StringBuilder attributesSB = new StringBuilder();
    StringBuilder valueSB = new StringBuilder();
    
    PrintWriter pw = resp.getWriter();
    
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();
      
      if (!decoder.next()) {
        continue;
      }
    
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the new name
      //

      boolean displayName = false;
      
      if (null == name || (!name.equals(decoder.getName()) || !labels.equals(lbls))) {
        displayName = true;
        name = decoder.getName();
        labels = lbls;
        classSB.setLength(0);
        GTSHelper.encodeName(classSB, name);
        labelsSB.setLength(0);
        attributesSB.setLength(0);
        boolean first = true;
        
        for (Entry<String, String> entry: lbls.entrySet()) {
          //
          // Skip owner/producer labels and any other 'private' labels
          //
          if (!signed) {
            if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
              continue;
            }
            if (Constants.OWNER_LABEL.equals(entry.getKey())) {
              continue;
            }            
          }
          
          if (!first) {
            labelsSB.append(",");
          }
          GTSHelper.encodeName(labelsSB, entry.getKey());
          labelsSB.append("=");
          GTSHelper.encodeName(labelsSB, entry.getValue());
          first = false;
        }

        first = true;
        if (decoder.getMetadata().getAttributesSize() > 0) {
          for (Entry<String, String> entry: decoder.getMetadata().getAttributes().entrySet()) {
            if (!first) {
              attributesSB.append(",");
            }
            GTSHelper.encodeName(attributesSB, entry.getKey());
            attributesSB.append("=");
            GTSHelper.encodeName(attributesSB, entry.getValue());
            first = false;
          }          
        }

      }
      
      long timestamp = 0L;
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;
      Object value = null;
      
      boolean dup = true;
      
      do {
        //
        // Filter out any value not in the time range
        //
        
        long newTimestamp = decoder.getTimestamp();
        
        if (newTimestamp > now || (timespan >= 0 && newTimestamp <= (now -timespan))) {
          continue;
        }
        
        //
        // TODO(hbs): filter out values with no location or outside the selected geozone when a geozone was set
        //
        
        long newLocation = decoder.getLocation();
        long newElevation = decoder.getElevation();
        Object newValue = decoder.getValue();
        
        dup = true;
        
        if (dedup) {
          if (location != newLocation || elevation != newElevation) {
            dup = false;
          } else {
            if (null == newValue) {
              // Consider nulls as duplicates (can't happen!)
              dup = false;
            } else if (newValue instanceof Number) {
              if (!((Number) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof String) {
              if (!((String) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof Boolean) {
              if (!((Boolean) newValue).equals(value)) {
                dup = false;
              }
            }
          }          
        }
                
        location = newLocation;
        elevation = newElevation;
        timestamp = newTimestamp;
        value = newValue;
            
        if (raw) {
          if (!dedup || !dup) {
            pw.print(classSB);
            pw.print('\t');
            pw.print(labelsSB);
            pw.print('\t');
            pw.print(attributesSB);
            pw.print('\t');
            
            pw.print(timestamp);
            pw.print('\t');
            
            if (GeoTimeSerie.NO_LOCATION != location) {
              double[] latlon = GeoXPLib.fromGeoXPPoint(location);
              pw.print(latlon[0]);
              pw.print('\t');
              pw.print(latlon[1]);
            } else {
              pw.print('\t');
            }

            pw.print('\t');

            if (GeoTimeSerie.NO_ELEVATION != elevation) {
              pw.print(elevation);
            }
            pw.print('\t');
            
            valueSB.setLength(0);
            GTSHelper.encodeValue(valueSB, value);
            pw.println(valueSB);
          }
        } else {
          // Display the name only if we have at least one value to display
          // We force 'dup' to be false when we must show the name
          if (displayName) {
            pw.print("# ");
            pw.print(classSB);
            pw.print("{");
            pw.print(labelsSB);
            pw.print("}");
            pw.print("{");
            pw.print(attributesSB);
            pw.println("}");
            displayName = false;
            dup = false;
          }
          
          if (!dedup || !dup) {
            pw.print(timestamp);
            pw.print('\t');           
            if (GeoTimeSerie.NO_LOCATION != location) {
              double[] latlon = GeoXPLib.fromGeoXPPoint(location);
              pw.print(latlon[0]);
              pw.print('\t');
              pw.print(latlon[1]);
            } else {
              pw.print('\t');
            }
            
            pw.print('\t');

            if (GeoTimeSerie.NO_ELEVATION != elevation) {
              pw.print(elevation);
            }
            pw.print('\t');
            
            valueSB.setLength(0);
            GTSHelper.encodeValue(valueSB, value);
            pw.println(valueSB);
          }
        }
      } while (decoder.next());        
      
      // Print any remaining value
      if (dedup && dup) {
        if (raw) {
          pw.print(classSB);
          pw.print('\t');
          pw.print(labelsSB);
          pw.print('\t');
          pw.print(attributesSB);
          pw.print('\t');
            
          pw.print(timestamp);
          pw.print('\t');

          if (GeoTimeSerie.NO_LOCATION != location) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(location);
            pw.print(latlon[0]);
            pw.print('\t');
            pw.print(latlon[1]);
          } else {
            pw.print('\t');
          }

          pw.print('\t');

          if (GeoTimeSerie.NO_ELEVATION != elevation) {
            pw.print(elevation);
          }
          pw.print('\t');
            
          valueSB.setLength(0);
          GTSHelper.encodeValue(valueSB, value);
          pw.println(valueSB);
        } else {
          pw.print(timestamp);
          pw.print('\t');
          if (GeoTimeSerie.NO_LOCATION != location) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(location);
            pw.print(latlon[0]);
            pw.print('\t');
            pw.print(latlon[1]);
          } else {
            pw.print('\t');
          }

          pw.print('\t');
          
          if (GeoTimeSerie.NO_ELEVATION != elevation) {
            pw.print(elevation);
          }
          pw.print('\t');
            
          valueSB.setLength(0);
          GTSHelper.encodeValue(valueSB, value);
          pw.println(valueSB);
        }
        
      }
      
      //
      // If displayName is still true it means we should have displayed the name but no value matched,
      // so set name to null so we correctly display the name for the next decoder if it has values
      //
      
      if (displayName) {
        name = null;
      }
    }    
  }

}
