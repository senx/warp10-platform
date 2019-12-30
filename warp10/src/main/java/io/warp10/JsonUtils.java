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

  /**
   * ObjectMapper instances are thread-safe, so we can safely use a single static instance.
   */
  private static final ObjectMapper strictMapper;
  private static final ObjectMapper looseMapper;

  static {
    JsonFactoryBuilder strictBuilder = new JsonFactoryBuilder();
    strictBuilder.enable(JsonWriteFeature.ESCAPE_NON_ASCII);
    strictMapper = new ObjectMapper(strictBuilder.build());
    strictMapper.getSerializerProvider().setNullKeySerializer(new NullKeySer());

    JsonFactoryBuilder looseBuilder = new JsonFactoryBuilder();
    looseBuilder.enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS);
    looseBuilder.enable(JsonReadFeature.ALLOW_MISSING_VALUES);
    looseBuilder.enable(JsonWriteFeature.ESCAPE_NON_ASCII);
    looseBuilder.disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    looseMapper = new ObjectMapper(looseBuilder.build());
    looseMapper.getSerializerProvider().setNullKeySerializer(new NullKeySer());
  }

  public static String objectToJson(Object o) throws JsonProcessingException {
    return objectToJson(o, true);
  }

  public static String objectToJson(Object o, boolean isStrict) throws JsonProcessingException {
    if (isStrict) {
      return objectToJson(o, strictMapper);
    } else {
      return objectToJson(o, looseMapper);
    }
  }

  private static String objectToJson(Object o, ObjectMapper mapper) throws JsonProcessingException {
    return mapper.writeValueAsString(o);
  }

  public static Object jsonToObject(String json) throws JsonProcessingException {
    return jsonToObject(json, true);
  }

  public static Object jsonToObject(String json, boolean isStrict) throws JsonProcessingException {
    if (isStrict) {
      return jsonToObject(json, strictMapper);
    } else {
      return jsonToObject(json, looseMapper);
    }
  }

  private static Object jsonToObject(String json, ObjectMapper mapper) throws JsonProcessingException {
    return mapper.readValue(json, Object.class);
  }

  public static void objectToJson(Appendable a, Object o, AtomicInteger recursionLevel, boolean isStrict) throws IOException {
    if (isStrict) {
      objectToJson(a, o, recursionLevel, strictMapper);
    } else {
      objectToJson(a, o, recursionLevel, looseMapper);
    }
  }

  private static void objectToJson(Appendable a, Object o, AtomicInteger recursionLevel, ObjectMapper mapper) throws IOException {

    if (recursionLevel.addAndGet(1) > WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL && ((o instanceof Map) || (o instanceof List) || (o instanceof WarpScriptStack.Macro))) {
      a.append(" ...NESTED_CONTENT_REMOVED... ");
      recursionLevel.addAndGet(-1);
      return;
    }

    if (o instanceof Number || o instanceof String || o instanceof Boolean) {
      a.append(objectToJson(o, mapper));
    } else if (o instanceof Map) {
      a.append("{");
      boolean first = true;
      for (Map.Entry keyAndValue: ((Map<?, ?>) o).entrySet()) {
        Object key = keyAndValue.getKey();
        if (!first) {
          a.append(",");
        }
        if (null != key) {
          a.append(objectToJson(key.toString(), mapper));
        } else {
          a.append("\"\"");
        }
        a.append(":");
        objectToJson(a, keyAndValue.getValue(), recursionLevel, mapper);
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
        objectToJson(a, elt, recursionLevel, mapper);
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
      a.append(objectToJson(name, mapper));
      a.append(",\"l\":");
      objectToJson(a, ((GeoTimeSerie) o).getMetadata().getLabels(), recursionLevel, mapper);
      a.append(",\"a\":");
      objectToJson(a, ((GeoTimeSerie) o).getMetadata().getAttributes(), recursionLevel, mapper);
      a.append(",\"la\":");
      objectToJson(a, ((GeoTimeSerie) o).getMetadata().getLastActivity(), recursionLevel, mapper);
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
        a.append(objectToJson(v, mapper));
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
      a.append(objectToJson(name, mapper));
      a.append(",\"l\":");
      objectToJson(a, ((GTSEncoder) o).getMetadata().getLabels(), recursionLevel, mapper);
      a.append(",\"a\":");
      objectToJson(a, ((GTSEncoder) o).getMetadata().getAttributes(), recursionLevel, mapper);
      a.append(",\"la\":");
      objectToJson(a, ((GTSEncoder) o).getMetadata().getLastActivity(), recursionLevel, mapper);
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
        a.append(objectToJson(v, mapper));
        a.append("]");
        first = false;
      }
      a.append("]");
      a.append("}");

    } else if (o instanceof Metadata) {
      a.append("{");
      a.append("\"c\":");
      a.append(objectToJson(((Metadata) o).getName(), mapper));
      a.append(",\"l\":");
      objectToJson(a, ((Metadata) o).getLabels(), recursionLevel, mapper);
      a.append(",\"a\":");
      objectToJson(a, ((Metadata) o).getAttributes(), recursionLevel, mapper);
      a.append("}");
    } else if (o instanceof WarpScriptStack.Macro) {
      a.append(objectToJson(o.toString(), mapper));
    } else if (o instanceof NamedWarpScriptFunction) {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(o.toString());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      a.append(objectToJson(sb.toString(), mapper));
    } else {
      a.append("null");
    }

    recursionLevel.addAndGet(-1);
  }

}
