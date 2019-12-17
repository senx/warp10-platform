//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10;

import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonUtils {

  private static class NullKeySer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeFieldName("");
    }
  }

  private static final ObjectMapper mapper;

  static {
    JsonFactoryBuilder looseBuilder = new JsonFactoryBuilder();
    looseBuilder.enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS);
    looseBuilder.enable(JsonReadFeature.ALLOW_MISSING_VALUES);
    looseBuilder.enable(JsonWriteFeature.ESCAPE_NON_ASCII);
    looseBuilder.disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    mapper = new ObjectMapper(looseBuilder.build());
    mapper.getSerializerProvider().setNullKeySerializer(new NullKeySer());
  }

  public static String objectToJson(Object o) throws JsonProcessingException {
    return mapper.writeValueAsString(o);
  }

  public static Object jsonToObject(String json) throws JsonProcessingException {
    return mapper.readValue(json, Object.class);
  }

  public static void objectToJson(Appendable a, Object o, AtomicInteger recursionLevel, boolean strictJSON) throws IOException {

    if (recursionLevel.addAndGet(1) > WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL && ((o instanceof Map) || (o instanceof List) || (o instanceof WarpScriptStack.Macro))) {
      a.append(" ...NESTED_CONTENT_REMOVED... ");
      recursionLevel.addAndGet(-1);
      return;
    }

    if (strictJSON && (o instanceof Double && (Double.isNaN((double) o) || Double.isInfinite((double) o)))) {
      a.append("null");
    } else if (strictJSON && (o instanceof Float && (Float.isNaN((float) o) || Float.isInfinite((float) o)))) {
      a.append("null");
    } else if (o instanceof Number || o instanceof String || o instanceof Boolean) {
      a.append(JsonUtils.objectToJson(o));
    } else if (o instanceof Map) {
      a.append("{");
      boolean first = true;
      for (Map.Entry keyAndValue: ((Map<?, ?>) o).entrySet()) {
        Object key = keyAndValue.getKey();
        if (!first) {
          a.append(",");
        }
        if (null != key) {
          a.append(JsonUtils.objectToJson(key.toString()));
        } else {
          a.append("null");
        }
        a.append(":");
        objectToJson(a, keyAndValue.getValue(), recursionLevel, strictJSON);
        first = false;
      }
      a.append("}");
    } else if (o instanceof List) {
      a.append("[");
      boolean first = true;
      for (Object elt: ((List) o)) {
        if (!first) {
          a.append(",");
        }
        objectToJson(a, elt, recursionLevel, strictJSON);
        first = false;
      }
      a.append("]");
    } else if (o instanceof GeoTimeSerie) {
      a.append("{");
      a.append("\"c\":");
      String name = ((GeoTimeSerie) o).getMetadata().getName();
      if (null == name) {
        name = "";
      }
      a.append(JsonUtils.objectToJson(name));
      a.append(",\"l\":");
      objectToJson(a, ((GeoTimeSerie) o).getMetadata().getLabels(), recursionLevel, strictJSON);
      a.append(",\"a\":");
      objectToJson(a, ((GeoTimeSerie) o).getMetadata().getAttributes(), recursionLevel, strictJSON);
      a.append(",\"la\":");
      objectToJson(a, ((GeoTimeSerie) o).getMetadata().getLastActivity(), recursionLevel, strictJSON);
      a.append(",\"v\":[");
      boolean first = true;
      for (int i = 0; i < ((GeoTimeSerie) o).size(); i++) {
        if (!first) {
          a.append(",");
        }
        long ts = GTSHelper.tickAtIndex((GeoTimeSerie) o, i);
        long location = GTSHelper.locationAtIndex((GeoTimeSerie) o, i);
        long elevation = GTSHelper.elevationAtIndex((GeoTimeSerie) o, i);
        Object v = GTSHelper.valueAtIndex((GeoTimeSerie) o, i);
        a.append("[");
        a.append(Long.toString(ts));
        if (GeoTimeSerie.NO_LOCATION != location) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          a.append(",");
          a.append(Double.toString(latlon[0]));
          a.append(",");
          a.append(Double.toString(latlon[1]));
        }
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          a.append(",");
          a.append(Long.toString(elevation));
        }
        a.append(",");
        if (strictJSON && (v instanceof Double) && (Double.isNaN((double) v) || Double.isInfinite((double) v))) {
          a.append("null");
        } else {
          a.append(JsonUtils.objectToJson(v));
        }
        a.append("]");
        first = false;
      }
      a.append("]");
      a.append("}");
    } else if (o instanceof GTSEncoder) {
      a.append("{");
      a.append("\"c\":");
      String name = ((GTSEncoder) o).getMetadata().getName();
      if (null == name) {
        name = "";
      }
      a.append(JsonUtils.objectToJson(name));
      a.append(",\"l\":");
      objectToJson(a, ((GTSEncoder) o).getMetadata().getLabels(), recursionLevel, strictJSON);
      a.append(",\"a\":");
      objectToJson(a, ((GTSEncoder) o).getMetadata().getAttributes(), recursionLevel, strictJSON);
      a.append(",\"la\":");
      objectToJson(a, ((GTSEncoder) o).getMetadata().getLastActivity(), recursionLevel, strictJSON);
      a.append(",\"v\":[");
      boolean first = true;
      GTSDecoder decoder = ((GTSEncoder) o).getUnsafeDecoder(false);
      while (decoder.next()) {
        if (!first) {
          a.append(",");
        }
        long ts = decoder.getTimestamp();
        long location = decoder.getLocation();
        long elevation = decoder.getElevation();
        // We do not call getBinaryValue because JSON cannot represent byte arrays
        Object v = decoder.getValue();
        a.append("[");
        a.append(Long.toString(ts));
        if (GeoTimeSerie.NO_LOCATION != location) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          a.append(",");
          a.append(Double.toString(latlon[0]));
          a.append(",");
          a.append(Double.toString(latlon[1]));
        }
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          a.append(",");
          a.append(Long.toString(elevation));
        }
        a.append(",");
        if (strictJSON && (v instanceof Double) && (Double.isNaN((double) v) || Double.isInfinite((double) v))) {
          a.append("null");
        } else {
          a.append(JsonUtils.objectToJson(v));
        }
        a.append("]");
        first = false;
      }
      a.append("]");
      a.append("}");

    } else if (o instanceof Metadata) {
      a.append("{");
      a.append("\"c\":");
      a.append(JsonUtils.objectToJson(((Metadata) o).getName()));
      a.append(",\"l\":");
      objectToJson(a, ((Metadata) o).getLabels(), recursionLevel, strictJSON);
      a.append(",\"a\":");
      objectToJson(a, ((Metadata) o).getAttributes(), recursionLevel, strictJSON);
      a.append("}");
    } else if (o instanceof WarpScriptStack.Macro) {
      a.append(JsonUtils.objectToJson(o.toString()));
    } else if (o instanceof NamedWarpScriptFunction) {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(o.toString());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      a.append(JsonUtils.objectToJson(sb.toString()));
    } else {
      a.append("null");
    }

    recursionLevel.addAndGet(-1);
  }

}
