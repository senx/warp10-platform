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
package io.warp10.plugins.influxdb;

import io.warp10.WarpURLDecoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBHandler.class);

  private final URL url;
  private final String token;
  private final String measurementlabel;

  private final int cachesize;
  private final long threshold;

  private static final long[] CLASS_KEYS  = new long[] { 0x496eL, 0x666cL }; // In fl
  private static final long[] LABELS_KEYS = new long[] { 0x7578L, 0x4442L }; // ux DB

  private static final String PARAM_PRECISION = "precision";
  private static final String PARAM_MLABEL = "mlabel";
  private static final String PARAM_PASSWORD = "p";

  public InfluxDBHandler(URL url, String token, String measurementlabel, long threshold, int cachesize) {
    this.url = url;
    this.token = token;
    this.measurementlabel = measurementlabel;
    this.cachesize = cachesize;
    this.threshold = threshold;
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    baseRequest.setHandled(true);

    // Only run code on REQUEST
    if(DispatcherType.REQUEST != baseRequest.getDispatcherType()){
      return;
    }

    long ilpTimeUnitMultiplier = 1;

    String qs = request.getQueryString();

    Map<String,String> params = null;

    if (null != qs) {
      params = Splitter.on("&").withKeyValueSeparator("=").split(qs);
    } else {
      params = new HashMap();
    }

    String precision = params.get(PARAM_PRECISION);

    if (null != precision) {
      if ("n".equals(precision)) {
        ilpTimeUnitMultiplier = 1;
      } else if ("u".equals(precision)) {
        ilpTimeUnitMultiplier = 1000L;
      } else if ("ms".equals(precision)) {
        ilpTimeUnitMultiplier = 1000000L;
      } else if ("s".equals(precision)) {
        ilpTimeUnitMultiplier = 1000000000L;
      } else if ("m".equals(precision)) {
        ilpTimeUnitMultiplier = 60 * 1000000000L;
      } else if ("h".equals(precision)) {
        ilpTimeUnitMultiplier = 3600 * 1000000000L;
      } else {
        throw new IOException("Invalid precision.");
      }
    }

    //
    // Adjust the ratio and time unit factor
    //

    long ratio = 1L;

    //
    // If the platform's time unit is not set to ns then adapt ratio and ilpTimeUnitMultiplier
    //

    if (1000000000L != Constants.TIME_UNITS_PER_S) {
      ratio = 1000000000L / Constants.TIME_UNITS_PER_S;

      // Divide each by 1000 until one of the ratios is 1
      while(ratio > 1L && ilpTimeUnitMultiplier > 1L) {
        ratio /= 1000L;
        ilpTimeUnitMultiplier /= 1000L;
      }
    }

    String token = this.token;

    String basicAuth = request.getHeader("Authorization");

    if (null != basicAuth) {
      String[] userpass = new String(Base64.decodeBase64(basicAuth.substring(6))).split(":");
      token = userpass[1];
    } else if (params.containsKey(PARAM_PASSWORD)) {
      token = params.get(PARAM_PASSWORD);
    }

    if (null == token) {
      throw new IOException("Missing password and no default token.");
    }

    //
    // By default we use the measurement label from the configuration.
    // If not set, we allow the request to specify one using a parameter
    //

    String mlabel = measurementlabel;

    if (null == mlabel) {
      mlabel = params.get(PARAM_MLABEL);
    }

    HttpURLConnection conn = null;

    try {

      conn = (HttpURLConnection) url.openConnection();

      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_UPDATE_TOKENX), token);
      //conn.setRequestProperty("Content-Type", "application/gzip");
      conn.setChunkedStreamingMode(16384);
      conn.connect();

      OutputStream os = conn.getOutputStream();
      //GZIPOutputStream out = new GZIPOutputStream(os);
      PrintWriter pw = new PrintWriter(os);

      String contentType = request.getHeader("Content-Type");
      BufferedReader br;

      if (null == contentType || !"application/gzip".equals(contentType)) {
        br = request.getReader();
      } else {
        GZIPInputStream gzin = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(gzin));
      }

      Map<UUID, GTSEncoder> currentEncoders = new HashMap<UUID, GTSEncoder>();
      AtomicReference<String> lastLabels = new AtomicReference(null);
      AtomicReference<String> lastMeasurement = new AtomicReference(null);
      AtomicReference<Map<String,String>> curLabels = new AtomicReference(null);

      long labelsId = 0L;
      long classId = 0L;

      Map<String,Long> classIds = new LinkedHashMap<String,Long>() {
        @Override
        protected boolean removeEldestEntry(java.util.Map.Entry<String, Long> eldest) {
          return this.size() > cachesize;
        }
      };

      long encsize = 0L;

      while(true) {
        encsize = parseMulti(br, mlabel, ilpTimeUnitMultiplier, ratio, threshold, classIds, currentEncoders, lastLabels, lastMeasurement, curLabels);

        if (0 == encsize) {
          break;
        }

        // If the cumulative size of encoders is above a threshold, flush all encoders
        if (encsize > this.threshold) {
          for (GTSEncoder encoder: currentEncoders.values()) {
            GTSHelper.dump(encoder, pw);
          }
          currentEncoders.clear();
          encsize = 0;
        }
      }

      pw.close();
      br.close();

      if (HttpServletResponse.SC_OK != conn.getResponseCode()) {
        throw new IOException(conn.getResponseMessage());
      }
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }
  }

  private static final List<GTSEncoder> NO_ENCODERS = Collections.unmodifiableList(new ArrayList<GTSEncoder>(0));

  public static List<GTSEncoder> parse(String ilp, long ratio, AtomicReference<String> lastmeasurement, AtomicReference<String> lastLabels, AtomicReference<Map<String,String>> curLabels, String mlabel, long ilpTimeUnitMultiplier) {
    ilp = ilp.trim();

    if ("".equals(ilp) || ilp.startsWith("#")) {
      return NO_ENCODERS;
    }

    // percent encode % signs
    if (-1 != ilp.indexOf('%')) {
      ilp.replaceAll("%", "%25");
    }

    // Replace escape sequences with percent encoding

    StringBuilder sb = new StringBuilder();

    int idx = ilp.indexOf('\\');
    int lastidx = 0;

    while(-1 != idx) {
      sb.append(ilp.subSequence(lastidx, idx));

      char c = ilp.charAt(idx + 1);

      if (' ' == c) {
        sb.append("%20");
        lastidx = idx + 2;
      } else if (',' == c) {
        sb.append("%2C");
        lastidx = idx + 2;
      } else if ('=' == c) {
        sb.append("%3D");
        lastidx = idx + 2;
      } else if ('\\' == c) {
        sb.append("%5C");
        lastidx = idx + 2;
      } else if ('\"' == c) {
        sb.append("%22");
        lastidx = idx + 2;
      } else {
        // This is simply a \
        sb.append("%5C");
        lastidx = idx + 1;
      }

      idx = ilp.indexOf('\\', lastidx);
    }

    if (lastidx < ilp.length()) {
      sb.append(ilp.substring(lastidx));
    }

    //
    // Parse the line, the format is
    // <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
    //

    // Find the first occurrence of space, it is the separator between measurement + tags and fields

    idx = sb.indexOf(" ", 0);

    // If there is a comma before idx, then it means there are tags defined

    Map<String,String> labels = curLabels.get();

    String measurement = null;

    int commaidx = sb.indexOf(",", 0);

    if (commaidx < idx) {
      measurement = sb.substring(0, commaidx).toString();

      String tagset = sb.substring(commaidx + 1, idx);

      // If the measurement is added to the labels, we need to check if the measurement changed since the
      // previous line on top of checking if the tagset changed.
      if (!tagset.equals(lastLabels.get()) || (null != mlabel && !measurement.equals(lastmeasurement.get()))) {
        labels = new LinkedHashMap<String,String>();
        String[] tags = tagset.split(",");

        try {
          for (String tag: tags) {
            int eqidx = tag.indexOf('=');
            String name = tag.substring(0, eqidx);
            if (-1 != name.indexOf('%')) {
              name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);
            }
            String value = tag.substring(eqidx + 1);
            if (-1 != value.indexOf('%')) {
              value = WarpURLDecoder.decode(value, StandardCharsets.UTF_8);
            }
            labels.put(name, value);
          }
        } catch (UnsupportedEncodingException uee) {
          throw new RuntimeException(uee);
        }
        lastLabels.set(tagset);
        curLabels.set(labels);
      }
    } else {
      measurement = sb.substring(0, idx).toString();
    }

    lastmeasurement.set(measurement);

    if (null != mlabel) {
      labels.put(mlabel, measurement);
    }

    //
    // Parse the timestamp
    //
    int tsidx = sb.lastIndexOf(" ");

    long ts = -1 != tsidx ? Long.parseLong(sb.substring(tsidx + 1)) * ilpTimeUnitMultiplier : TimeSource.getTime();

    // Adjust the time units if there was a timestamp specified AND the ratio is not 1L
    if (1L != ratio && -1 != tsidx) {
      ts = ts / ratio;
    }

    //
    // Parse the fields
    //

    String fieldset = sb.substring(idx + 1, tsidx);

    List<GTSEncoder> encoders = new ArrayList<GTSEncoder>();

    try {
      int eqidx = fieldset.indexOf('=');
      int startidx = 0;

      while(-1 != eqidx) {
        GTSEncoder encoder = new GTSEncoder(0);
        String name = fieldset.substring(startidx, eqidx);
        if (-1 != name.indexOf('%')) {
          name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);
        }

        if (null == mlabel) {
          encoder.setName(measurement + "." + name);
        } else {
          encoder.setName(name);
        }

        String value = null;
        Object o = null;

        if ('\"' == fieldset.charAt(eqidx + 1)) {
          // We can check the for end " since escaped double quotes were percent encoded
          startidx = fieldset.indexOf('\"', eqidx + 2);
          value = fieldset.substring(eqidx + 2, startidx);
          if ( -1 != value.indexOf('%')) {
            value = WarpURLDecoder.decode(value, StandardCharsets.UTF_8);
          }
          o = value;
          startidx = startidx + 2; // skipping a comma and "
        } else {
          int cidx = fieldset.indexOf(',', eqidx + 1);
          if (-1 != cidx) {
            value = fieldset.substring(eqidx + 1, cidx);
            startidx = cidx + 1;
          } else {
            value = fieldset.substring(eqidx + 1);
            startidx = fieldset.length();
          }
          if (value.endsWith("i")) {
            o = Long.parseLong(value.substring(0, value.length() - 1));
          } else if (value.endsWith("u")) { // Unsigned values are unsupported, we need to convert them to signed
            BigInteger bi = new BigInteger(value.substring(0, value.length() - 1));
            try {
              o = bi.longValueExact();
            } catch (ArithmeticException ae) {
              o = bi.subtract(BigInteger.valueOf(Long.MAX_VALUE)).longValueExact();
            }
          } else if (value.startsWith("t") || value.startsWith("T")) {
            o = Boolean.TRUE;
          } else if (value.startsWith("f") || value.startsWith("F")) {
            o = Boolean.FALSE;
          } else { // Floating point number
            o = Double.parseDouble(value);
          }
        }
        encoder.addValue(ts, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, o);
        encoder.setLabels(labels);
        encoders.add(encoder);
        eqidx = fieldset.indexOf('=', startidx);
      }

      return encoders;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private static long parseMulti(BufferedReader br, String mlabel, long ilpTimeUnitMultiplier, long ratio, long threshold, Map<String,Long> classIds, Map<UUID,GTSEncoder> currentEncoders, AtomicReference<String> lastLabels, AtomicReference<String> lastMeasurement, AtomicReference<Map<String,String>> curLabels) throws IOException {

    long labelsId = 0L;
    long classId = 0L;
    long encsize = 0L;

    while(true) {
      String line = br.readLine();
      if (null == line) {
        break;
      }
      try {
        String parsedLabels = lastLabels.get();
        String parsedMeasurement = lastMeasurement.get();

        List<GTSEncoder> encoders = parse(line, ratio, lastMeasurement, lastLabels, curLabels, mlabel, ilpTimeUnitMultiplier);

        if (encoders.isEmpty()) {
          continue;
        }

        Map<String,String> labels = encoders.get(0).getRawMetadata().getLabels();

        if (!lastLabels.get().equals(parsedLabels) || (null != mlabel && !lastMeasurement.get().equals(parsedMeasurement))) {
          // Remove producer/owner/app
          labels.remove(Constants.PRODUCER_LABEL);
          labels.remove(Constants.OWNER_LABEL);
          labels.remove(Constants.APPLICATION_LABEL);

          // Compute labelsId
          labelsId = GTSHelper.labelsId(LABELS_KEYS, labels);
          parsedLabels = lastLabels.get();
        }

        for (GTSEncoder encoder: encoders) {
          // Compute classid if it changed
          String cls = encoder.getRawMetadata().getName();
          if (!classIds.containsKey(cls)) {
            classId = GTSHelper.classId(CLASS_KEYS, cls);
            classIds.put(cls, classId);
          } else {
            classId = classIds.get(cls);
          }

          UUID uuid = new UUID(classId, labelsId);

          if (!currentEncoders.containsKey(uuid)) {
            currentEncoders.put(uuid, encoder);
            encsize += encoder.size();
          } else {
            GTSDecoder decoder = encoder.getDecoder(true);
            GTSEncoder enco = currentEncoders.get(uuid);
            long cursize = enco.size();
            while(decoder.next()) {
              enco.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
            }
            encsize -= cursize;
            encsize += enco.size();
          }
        }

        if (encsize > threshold) {
          break;
        }
      } catch (Throwable t) {
        LOG.error("Error while parsing '" + line + "'.", t);
        throw t;
      }
    }

    return encsize;
  }
}
