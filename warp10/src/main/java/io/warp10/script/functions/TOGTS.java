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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.MetadataSelectorMatcher;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * Converts an encoder into a map of gts, one per type
 */
public class TOGTS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOGTS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Map<String, MetadataSelectorMatcher> typeMap = null;
    Object top = stack.pop();

    if (top instanceof Map) {
      typeMap = new LinkedHashMap<>();
      // this is a map to specify type by name, it should contain valid types
      for (Map.Entry<Object, Object> entry: ((Map<Object, Object>) top).entrySet()) {
        if (!(entry.getValue() instanceof String)) {
          throw new WarpScriptException(getName() + " type MAP input must contains selector string as values.");
        }
        if (entry.getKey() instanceof String) {
          String t = (String) entry.getKey();
          if ("LONG".equals(t) || "DOUBLE".equals(t) || "BOOLEAN".equals(t) || "STRING".equals(t)) {
            typeMap.put((String) entry.getKey(), new MetadataSelectorMatcher((String) entry.getValue()));
          } else {
            throw new WarpScriptException(getName() + " type MAP input must contains valid types as key (LONG, DOUBLE, BOOLEAN or STRING).");
          }
        } else {
          throw new WarpScriptException(getName() + " type MAP input must contains valid types as key (LONG, DOUBLE, BOOLEAN or STRING).");
        }
      }
      top = stack.pop();
    }

    List<GTSDecoder> decodersInput = new ArrayList<>();

    boolean listInput = false;

    if (top instanceof List) {
      for (Object o: (List) top) {
        if (!(o instanceof String) && !(o instanceof byte[]) && !(o instanceof GTSEncoder)) {
          throw new WarpScriptException(getName() + " operates on a string, a byte array, an encoder, or list of thereof.");
        }
        decodersInput.add(getDecoderFromObject(o));
      }
      listInput = true;
    } else if (top instanceof String || top instanceof byte[] || top instanceof GTSEncoder) {
      decodersInput.add(getDecoderFromObject(top));
    } else {
      throw new WarpScriptException(getName() + " operates on a string, a byte array, an encoder, or list of thereof.");
    }

    // if there is no type map, the output is a map of GTS (v2.3.0 signature), or a map of lists of GTS
    if (null == typeMap) {
      Map<String, ArrayList<GeoTimeSerie>> result = new HashMap<>();
      for (GTSDecoder decoder: decodersInput) {
        Map<String, GeoTimeSerie> series = new HashMap<>();
        GeoTimeSerie gts;
        while (decoder.next()) {
          Object value = decoder.getBinaryValue();

          String type = "DOUBLE";

          if (value instanceof String) {
            type = "STRING";
          } else if (value instanceof Boolean) {
            type = "BOOLEAN";
          } else if (value instanceof Long) {
            type = "LONG";
          } else if (value instanceof Double || value instanceof BigDecimal) {
            type = "DOUBLE";
          } else if (value instanceof byte[]) {
            type = "BINARY";
          }

          gts = series.get(type);

          if (null == gts) {
            gts = new GeoTimeSerie();
            gts.setMetadata(decoder.getMetadata());
            series.put(type, gts);
          }

          GTSHelper.setValue(gts, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), value, false);
        }
        // exit here if input is not a list.
        if (!listInput) {
          stack.push(series);
          return stack;
        }
        // merge the series Map into the big one
        for (Entry<String, GeoTimeSerie> entry: series.entrySet()) {
          if (!result.containsKey(entry.getKey())) {
            result.put(entry.getKey(), new ArrayList<GeoTimeSerie>());
          }
          result.get(entry.getKey()).add(entry.getValue());
        }
      }
      stack.push(result);
      return stack;
    }

    // if there is a type map on the stack:
    //  - the map key is a selector. If the encoder's metadata fits, the gts will have the corresponding type of the first matching selector.
    //  - if the encoder doesn't fit to any selector, the gts will have the type of the first encountered element in the encoder.
    // GTSHelper.setValue will try to convert values whenever possible
    // a byte array will be serialized as an ISO-8859-1 string.
    else {
      ArrayList<GeoTimeSerie> result = new ArrayList<>();
      boolean classMatch;
      boolean labelsMatch;
      for (GTSDecoder decoder: decodersInput) {
        GeoTimeSerie gts = new GeoTimeSerie();
        gts.setMetadata(decoder.getMetadata());
        String enforcedType = null;
        for (Entry<String, MetadataSelectorMatcher> entry: typeMap.entrySet()) {
          if (entry.getValue().MetaDataMatch(decoder.getMetadata())) {
            enforcedType = entry.getKey();
          }
        }
        if (enforcedType != null) {
          if ("DOUBLE".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.DOUBLE);
          } else if ("LONG".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.LONG);
          } else if ("STRING".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.STRING);
          } else if ("BOOLEAN".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.BOOLEAN);
          }
        }
        while (decoder.next()) {
          Object value = decoder.getBinaryValue();
          if (value instanceof byte[]) {
            value = new String((byte[]) value, StandardCharsets.ISO_8859_1);
          }
          GTSHelper.setValue(gts, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), value, false);
        }
        if (!listInput) {
          stack.push(gts);
          return stack;
        }
        result.add(gts);
      }
      stack.push(result);
    }
    return stack;
  }

  private GTSDecoder getDecoderFromObject(Object o) throws WarpScriptException {
    GTSDecoder decoder;
    if (o instanceof GTSEncoder) {
      decoder = ((GTSEncoder) o).getUnsafeDecoder(false);
    } else {
      try {
        byte[] bytes = o instanceof String ? OrderPreservingBase64.decode(o.toString().getBytes(StandardCharsets.US_ASCII)) : (byte[]) o;
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        GTSWrapper wrapper = new GTSWrapper();
        deser.deserialize(wrapper, bytes);
        decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
      } catch (TException te) {
        throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
      }
    }
    return decoder;
  }

}
