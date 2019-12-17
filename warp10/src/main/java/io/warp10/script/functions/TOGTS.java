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

    Map<String, String> typeMap = null;
    Object top = stack.pop();

    if (top instanceof Map) {
      typeMap = new LinkedHashMap<>();
      // this is a map to specify type by name, it should contain valid types
      for (Map.Entry<Object, Object> entry: ((Map<Object, Object>) top).entrySet()) {
        if (!(entry.getKey() instanceof String)) {
          throw new WarpScriptException(getName() + " type MAP input must contains encoder name as keys.");
        }
        if (entry.getValue() instanceof String) {
          String t = (String) entry.getValue();
          if (t.equals("LONG") || t.equals("DOUBLE") || t.equals("BOOLEAN") || t.equals("STRING")) {
            typeMap.put((String) entry.getKey(), (String) entry.getValue());
          } else {
            throw new WarpScriptException(getName() + " type MAP input must contains valid types as value (LONG, DOUBLE, BOOLEAN or STRING).");
          }
        } else {
          throw new WarpScriptException(getName() + " type MAP input must contains valid types as value (LONG, DOUBLE, BOOLEAN or STRING).");
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
        for (Entry<String, String> entry: typeMap.entrySet()) {
          if (metadataMatchSelector(decoder.getMetadata(), entry.getKey())) {
            enforcedType = entry.getValue();
          }
        }
        if (enforcedType != null) {
          if (enforcedType.equals("DOUBLE")) {
            gts.setType(GeoTimeSerie.TYPE.DOUBLE);
          } else if (enforcedType.equals("LONG")) {
            gts.setType(GeoTimeSerie.TYPE.LONG);
          } else if (enforcedType.equals("STRING")) {
            gts.setType(GeoTimeSerie.TYPE.STRING);
          } else if (enforcedType.equals("BOOLEAN")) {
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

  /**
   * Returns true when the metadata match the selector.
   *
   * @param metadata Encoder/GeoTimeSerie metadata
   * @param selector Standard WarpScript selector, i.e. "~.*temperature{room=A,sensorplace~(ceil|roof)}"
   * @return true when metadata's classname, labels and attributes fits the selector.
   * @throws WarpScriptException
   */
  public boolean metadataMatchSelector(Metadata metadata, String selector) throws WarpScriptException {

    // PARSESELECTOR.parse() returns a string and a map. handle the url encoding.
    Object[] selectors = PARSESELECTOR.parse(selector);
    if (!(selectors[0] instanceof String) || !(selectors[1] instanceof Map)) {
      throw new WarpScriptException(getName() + " expects valid selectors in the type MAP.");
    }
    String classSelector = (String) selectors[0];
    Map<String, String> labelSelector = (Map) selectors[1];

    //check classname match
    boolean classMatch = classSelector.equals("~.*")
        || (classSelector.equals(metadata.getName()))
        || (classSelector.startsWith("=") && metadata.getName().equals(classSelector.substring(1)))
        || (classSelector.startsWith("~") && Pattern.compile(classSelector.substring(1)).matcher(metadata.getName()).matches());

    //check labels and attributes match
    boolean labelsMatch = (labelSelector.size() == 0);
    if (classMatch && !labelsMatch) {
      //build patterns from label selector map.
      Map labelPatterns = new HashMap<String, Pattern>();
      for (Entry<String, String> entry: labelSelector.entrySet()) {
        Pattern pattern;
        if (entry.getValue().startsWith("=")) {
          pattern = Pattern.compile(Pattern.quote(entry.getValue().substring(1)));
        } else if (entry.getValue().startsWith("~")) {
          pattern = Pattern.compile(entry.getValue().substring(1));
        } else {
          pattern = Pattern.compile(Pattern.quote(entry.getValue()));
        }
        labelPatterns.put(entry.getKey(), pattern);
      }

      //check matching labels,then matching attributes
      labelsMatch = true;
      for (Entry<String, String> entry: metadata.getLabels().entrySet()) {
        if (labelPatterns.containsKey(entry.getKey())) {
          labelsMatch &= ((Pattern) labelPatterns.get(entry.getKey())).matcher(entry.getValue()).matches();
          if (!labelsMatch) {
            break;
          }
        }
      }
      if (labelsMatch) {
        for (Entry<String, String> entry: metadata.getAttributes().entrySet()) {
          if (labelPatterns.containsKey(entry.getKey())) {
            labelsMatch &= ((Pattern) labelPatterns.get(entry.getKey())).matcher(entry.getValue()).matches();
            if (!labelsMatch) {
              break;
            }
          }
        }
      }
    }


    return classMatch && labelsMatch;
  }
}
