//
//   Copyright 2020-2023  SenX S.A.S.
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

package io.warp10.json;

import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.impl.UnknownSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class JsonUtils {

  /**
   * A serializer for null keys.
   * Outputs "null" because most javascript engines coerce null to "null" when using it as a key.
   */
  private static class NullKeySerializer extends JsonSerializer<Object> {
    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeFieldName("null");
    }
  }

  /**
   * Used to swap UnknownSerializer and BeanSerializer for CustomSerializer.
   */
  public static class NotSerializedToCustomSerializedModifier extends BeanSerializerModifier {
    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      if (serializer instanceof UnknownSerializer || serializer instanceof BeanSerializer) {
        return CUSTOM_SERIALIZER;
      } else {
        return serializer;
      }
    }
  }

  /**
   * Handles custom serialization based on transformers.
   */
  public static class CustomSerializer extends JsonSerializer<Object> {
    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      if (null != transformers && !transformers.isEmpty()) {
        JsonTransformer.TransformationResult transfRes = null;
        for (JsonTransformer transformer: transformers) {
          transfRes = transformer.transform(value);
          if (null != transfRes && transfRes.transformed) {
            break;
          }
        }
        if (null != transfRes && transfRes.transformed) {
          if (transfRes.raw && transfRes.result instanceof String) {
            gen.writeRawValue((String) transfRes.result);
          } else {
            gen.writeObject(transfRes.result);
          }
        } else {
          // No custom encoders able to encode this object, write null.
          gen.writeNull();
        }
      } else {
        // No custom encoders defined, write null.
        gen.writeNull();
      }
    }
  }

  private static final NullKeySerializer NULL_KEY_SERIALIZER = new NullKeySerializer();
  private static final CustomSerializer CUSTOM_SERIALIZER = new CustomSerializer();

  //
  // ObjectMapper instances are thread-safe, so we can safely use a single static instance.
  //
  private static final ObjectMapper STRICT_MAPPER;
  private static final ObjectMapper STRICT_MAPPER_PRETTY;
  private static final ObjectMapper LOOSE_MAPPER;
  private static final ObjectMapper LOOSE_MAPPER_PRETTY;

  public interface JsonTransformer {
    class TransformationResult {

      /**
       * Whether the transformer managed to transform the given object or not.
       * If false, result and raw fields are ignored.
       */
      private final boolean transformed;

      /**
       * The result of the transformation.
       * If raw, the result must be a String and will be added as is in the resulting JSON.
       * If not raw, the result should be an Object which can be JSONified. Typically Maps, Lists, String, Numbers, Booleans
       * or even other Objects which are known to have serializer (GTS, Bytes, ...) or handled by other transformers.
       */
      private final Object result;

      /**
       * Whether the result is to be added as is in the resulting JSON.
       * If true, result must be a String.
       * If false, result can be any Object which will be JSONified.
       */
      private final boolean raw;

      public TransformationResult(boolean transformed, Object result, boolean raw) {
        this.transformed = transformed;
        this.result = result;
        this.raw = raw;
      }
    }

    /**
     * Asks a JsonTransformer implementation if the given object is handled (TransformationResult#transformed=true)
     * and if it is, the result of the transformation (TransformationResult#result).
     * This allows the JSON serialization to ask for the conversion of un-serializable objects to serializable ones.
     * If it is not handled, TransformationResult#result is ignored, so it can be safely set to null.
     *
     * @param original The object to be transformed.
     * @return The result of the transformation.
     */
    TransformationResult transform(Object original);
  }

  private static List<JsonTransformer> transformers;

  static {
    //
    // Configure a module to handle the serialization of non-base classes.
    //
    SimpleModule module = new SimpleModule();
    // Add the NotSerializedToCustomSerializedModifier instance
    module.setSerializerModifier(new NotSerializedToCustomSerializedModifier());
    // Add core custom serializers
    module.addSerializer(new GeoTimeSerieSerializer());
    module.addSerializer(new GTSEncoderSerializer());
    module.addSerializer(new MetadataSerializer());
    module.addSerializer(new NamedWarpScriptFunctionSerializer());
    module.addSerializer(new MacroSerializer());
    module.addSerializer(new BytesSerializer());
    module.addSerializer(new RealVectorSerializer());
    module.addSerializer(new RealMatrixSerializer());
    module.addSerializer(new COWListSerializer());
    module.addSerializer(new WarpScriptAuditStatementSerializer());

    //
    // Common configuration for both strict and loose mappers.
    //
    JsonFactoryBuilder builder = new JsonFactoryBuilder();
    builder.enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS);
    builder.enable(JsonReadFeature.ALLOW_MISSING_VALUES);
    builder.enable(JsonWriteFeature.ESCAPE_NON_ASCII);
    builder.disable(JsonWriteFeature.WRITE_NUMBERS_AS_STRINGS);
    builder.disable(StreamWriteFeature.AUTO_CLOSE_TARGET);

    //
    // Configure strict mapper
    //
    builder.enable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    STRICT_MAPPER = new ObjectMapper(builder.build());
    STRICT_MAPPER.getSerializerProvider().setNullKeySerializer(NULL_KEY_SERIALIZER);
    STRICT_MAPPER.registerModule(module);
    // Pretty version
    STRICT_MAPPER_PRETTY = new ObjectMapper(builder.build());
    STRICT_MAPPER_PRETTY.enable(SerializationFeature.INDENT_OUTPUT);
    STRICT_MAPPER_PRETTY.getSerializerProvider().setNullKeySerializer(NULL_KEY_SERIALIZER);
    STRICT_MAPPER_PRETTY.registerModule(module);

    //
    // Configure loose mapper
    //
    builder.disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    LOOSE_MAPPER = new ObjectMapper(builder.build());
    LOOSE_MAPPER.getSerializerProvider().setNullKeySerializer(NULL_KEY_SERIALIZER);
    LOOSE_MAPPER.registerModule(module);
    // Pretty version
    LOOSE_MAPPER_PRETTY = new ObjectMapper(builder.build());
    LOOSE_MAPPER_PRETTY.enable(SerializationFeature.INDENT_OUTPUT);
    LOOSE_MAPPER_PRETTY.getSerializerProvider().setNullKeySerializer(NULL_KEY_SERIALIZER);
    LOOSE_MAPPER_PRETTY.registerModule(module);
  }

  //
  // Method to deserialize JSON to Objects.
  //

  public static Object jsonToObject(String json) throws JsonProcessingException {
    return STRICT_MAPPER.readValue(json, Object.class);
  }

  //
  // Methods to serialize objects to JSON
  //

  public static String objectToJson(Object o) throws IOException {
    return objectToJson(o, Long.MAX_VALUE);
  }

  public static String objectToJson(Object o, long maxJsonSize) throws IOException {
    return objectToJson(o, false, false, maxJsonSize);
  }

  public static String objectToJson(Object o, boolean isStrict) throws IOException {
    return objectToJson(o, isStrict, false, Long.MAX_VALUE);
  }

  public static String objectToJson(Object o, boolean isStrict, boolean isPretty) throws IOException {
    return objectToJson(o, isStrict, isPretty, Long.MAX_VALUE);
  }

  public static String objectToJson(Object o, boolean isStrict, long maxJsonSize) throws IOException {
    return objectToJson(o, isStrict, false, maxJsonSize);
  }

  public static String objectToJson(Object o, boolean isStrict, boolean isPretty, long maxJsonSize) throws IOException {
    StringWriter writer = new StringWriter();
    objectToJson(writer, o, isStrict, isPretty, maxJsonSize);
    return writer.toString();
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict) throws IOException {
    objectToJson(writer, o, isStrict, false, Long.MAX_VALUE);
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict, boolean isPretty) throws IOException {
    objectToJson(writer, o, isStrict, isPretty, Long.MAX_VALUE);
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict, long maxJsonSize) throws IOException {
    objectToJson(writer, o, isStrict, false, maxJsonSize);
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict, boolean isPretty, long maxJsonSize) throws IOException {
    if (Long.MAX_VALUE != maxJsonSize) {
      writer = new BoundedWriter(writer, maxJsonSize);
    }

    try {
      if (isStrict) {
        if (isPretty) {
          STRICT_MAPPER_PRETTY.writeValue(writer, o);
        } else {
          STRICT_MAPPER.writeValue(writer, o);
        }
      } else {
        if (isPretty) {
          LOOSE_MAPPER_PRETTY.writeValue(writer, o);
        } else {
          LOOSE_MAPPER.writeValue(writer, o);
        }
      }
    } catch (BoundedWriter.WriterBoundReachedException wbre) {
      throw new IOException("Resulting JSON is too big.", wbre);
    }
  }

  /**
   * Add a transformer to convert un-serializable objects to serializable ones or raw Strings included in the JSON.
   *
   * @param transformer The transformer instance.
   */
  public static synchronized void addTransformer(JsonTransformer transformer) {
    if (null == transformers) {
      transformers = new ArrayList<JsonTransformer>();
    }
    transformers.add(transformer);
  }

}
