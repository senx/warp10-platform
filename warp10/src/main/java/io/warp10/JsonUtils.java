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
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;

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
  private static final ObjectMapper mapper;

  static {

    JsonFactoryBuilder builder = new JsonFactoryBuilder();
    builder.enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS);
    builder.enable(JsonReadFeature.ALLOW_MISSING_VALUES);
    mapper = new ObjectMapper(builder.build());
    mapper.getSerializerProvider().setNullKeySerializer(new NullKeySer());
  }

  public static Object jsonToObject(String json) throws JsonProcessingException {
    return mapper.readValue(json, Object.class);
  }

  public static String objectToJson(Object o) throws IOException {
    return objectToJson(o, false);
  }

  public static String objectToJson(Object o, boolean isStrict) throws IOException {
    StringWriter writer = new StringWriter();
    objectToJson(writer, o, isStrict);
    return writer.toString();
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict) throws IOException {
    objectToJson(writer, o, isStrict, Long.MAX_VALUE);
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict, long allowedNesting) throws IOException {
    if (allowedNesting < 0) {
      writer.write(" ...NESTED_CONTENT_REMOVED... ");
      return;
    }

    if (null == o) {
      writer.write("null");
    } else if (o instanceof Boolean) {
      writer.write(o.toString());
    } else if (o instanceof String) {
      writer.write("\"");
      StringEscapeUtils.escapeJava(writer, (String) o);
      writer.write("\"");
    } else if (o instanceof Number) {
      if (isStrict && o instanceof Double && !Double.isFinite((Double) o)) {
        writer.write("\"");
        writer.write(o.toString());
        writer.write("\"");
      } else {
        writer.write(o.toString());
      }
    } else if (o instanceof Map) {
      writer.write("{");
      boolean first = true;
      for (Map.Entry keyAndValue: ((Map<?, ?>) o).entrySet()) {
        if (!first) {
          writer.write(",");
        }

        // Key must be a string. Even if in JavaScript it is possible to use numbers or null as key, they are in fact
        // coerced to string.
        Object key = keyAndValue.getKey();
        if (null == key) {
          objectToJson(writer, "null", isStrict, allowedNesting - 1);
        } else {
          objectToJson(writer, key.toString(), isStrict, allowedNesting - 1);
        }
        writer.write(":");
        objectToJson(writer, keyAndValue.getValue(), isStrict, allowedNesting - 1);
        first = false;
      }
      writer.write("}");
    } else if (o instanceof List) {
      writer.write("[");
      boolean first = true;
      for (Object elt: ((List) o)) {
        if (!first) {
          writer.write(",");
        }
        objectToJson(writer, elt, isStrict, allowedNesting - 1);
        first = false;
      }
      writer.write("]");
    } else if (o instanceof GeoTimeSerie) {
      writer.write("{");
      writer.write("\"c\":");
      String name = ((GeoTimeSerie) o).getMetadata().getName();
      if (null == name) {
        name = "";
      }
      objectToJson(writer, name, isStrict, allowedNesting - 1);
      writer.write(",\"l\":");
      objectToJson(writer, ((GeoTimeSerie) o).getMetadata().getLabels(), isStrict, allowedNesting - 1);
      writer.write(",\"a\":");
      objectToJson(writer, ((GeoTimeSerie) o).getMetadata().getAttributes(), isStrict, allowedNesting - 1);
      writer.write(",\"la\":");
      objectToJson(writer, ((GeoTimeSerie) o).getMetadata().getLastActivity(), isStrict, allowedNesting - 1);
      writer.write(",\"v\":[");
      boolean first = true;
      for (int i = 0; i < ((GeoTimeSerie) o).size(); i++) {
        if (!first) {
          writer.write(",");
        }
        long ts = GTSHelper.tickAtIndex((GeoTimeSerie) o, i);
        long location = GTSHelper.locationAtIndex((GeoTimeSerie) o, i);
        long elevation = GTSHelper.elevationAtIndex((GeoTimeSerie) o, i);
        Object v = GTSHelper.valueAtIndex((GeoTimeSerie) o, i);
        writer.write("[");
        writer.write(Long.toString(ts));
        if (GeoTimeSerie.NO_LOCATION != location) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          writer.write(",");
          writer.write(Double.toString(latlon[0]));
          writer.write(",");
          writer.write(Double.toString(latlon[1]));
        }
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          writer.write(",");
          writer.write(Long.toString(elevation));
        }
        writer.write(",");
        objectToJson(writer, v, isStrict, allowedNesting - 1);
        writer.write("]");
        first = false;
      }
      writer.write("]");
      writer.write("}");
    } else if (o instanceof GTSEncoder) {
      writer.write("{");
      writer.write("\"c\":");
      String name = ((GTSEncoder) o).getMetadata().getName();
      if (null == name) {
        name = "";
      }
      objectToJson(writer, name, isStrict, allowedNesting - 1);
      writer.write(",\"l\":");
      objectToJson(writer, ((GTSEncoder) o).getMetadata().getLabels(), isStrict, allowedNesting - 1);
      writer.write(",\"a\":");
      objectToJson(writer, ((GTSEncoder) o).getMetadata().getAttributes(), isStrict, allowedNesting - 1);
      writer.write(",\"la\":");
      objectToJson(writer, ((GTSEncoder) o).getMetadata().getLastActivity(), isStrict, allowedNesting - 1);
      writer.write(",\"v\":[");
      boolean first = true;
      GTSDecoder decoder = ((GTSEncoder) o).getUnsafeDecoder(false);
      while (decoder.next()) {
        if (!first) {
          writer.write(",");
        }
        long ts = decoder.getTimestamp();
        long location = decoder.getLocation();
        long elevation = decoder.getElevation();
        // We do not call getBinaryValue because JSON cannot represent byte arrays
        Object v = decoder.getValue();
        writer.write("[");
        writer.write(Long.toString(ts));
        if (GeoTimeSerie.NO_LOCATION != location) {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          writer.write(",");
          writer.write(Double.toString(latlon[0]));
          writer.write(",");
          writer.write(Double.toString(latlon[1]));
        }
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          writer.write(",");
          writer.write(Long.toString(elevation));
        }
        writer.write(",");
        objectToJson(writer, v, isStrict, allowedNesting - 1);
        writer.write("]");
        first = false;
      }
      writer.write("]");
      writer.write("}");

    } else if (o instanceof Metadata) {
      writer.write("{");
      writer.write("\"c\":");
      objectToJson(writer, ((Metadata) o).getName(), isStrict, allowedNesting - 1);
      writer.write(",\"l\":");
      objectToJson(writer, ((Metadata) o).getLabels(), isStrict, allowedNesting - 1);
      writer.write(",\"a\":");
      objectToJson(writer, ((Metadata) o).getAttributes(), isStrict, allowedNesting - 1);
      writer.write(",\"la\":");
      objectToJson(writer, ((Metadata) o).getLastActivity(), isStrict, allowedNesting - 1);
      writer.write("}");
    } else if (o instanceof WarpScriptStack.Macro) {
      objectToJson(writer, o.toString(), isStrict, allowedNesting - 1);
    } else if (o instanceof NamedWarpScriptFunction) {
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(o.toString());
      sb.append(" ");
      sb.append(WarpScriptStack.MACRO_END);
      sb.append(" ");
      sb.append(WarpScriptLib.EVAL);
      objectToJson(writer, sb.toString(), isStrict, allowedNesting - 1);
    } else {
      writer.write("null");
    }
  }

}
