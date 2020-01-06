//
//   Copyright 2020  SenX S.A.S.
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
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.impl.UnknownSerializer;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

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
   * A serializer that writes null whatever it is given.
   * This serializer is used to output null for instances of classes without specific serializer.
   */
  private static class NullSerializer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeNull();
    }
  }

  /**
   * Used to swap UnknownSerializer for NullSerializer.
   * This effectively allows a mapper to output null for any instance of class without a specific serializer.
   * This includes all Objects except for Numbers, Strings, Booleans, Lists, Maps, etc and Objects with
   * custom serializers added through Module.addSerializer.
   */
  public static class UnknownToNullSerializerModifier extends BeanSerializerModifier {
    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      if (serializer instanceof UnknownSerializer) {
        return nullSerializer;
      } else {
        return serializer;
      }
    }
  }

  public static final int MAX_JSON_SIZE_DEFAULT = 100 * 1024 * 1024;
  public static final JsonSerializer<Object> nullSerializer = new NullSerializer();

  //
  // ObjectMapper instances are thread-safe, so we can safely use a single static instance.
  //
  private static final ObjectMapper strictMapper;
  private static final ObjectMapper looseMapper;

  static {
    //
    // Configure a module to handle the serialization of non-base classes.
    //
    SimpleModule module = new SimpleModule();
    // Add the UnknownToNullSerializerModifier instance
    module.setSerializerModifier(new UnknownToNullSerializerModifier());
    // Add custom serializers
    module.addSerializer(GeoTimeSerie.class, new GeoTimeSerieSerializer());
    module.addSerializer(GTSEncoder.class, new GTSEncoderSerializer());
    module.addSerializer(Metadata.class, new MetadataSerializer());
    module.addSerializer(NamedWarpScriptFunction.class, new NamedWarpScriptFunctionSerializer());
    module.addSerializer(WarpScriptStack.Macro.class, new MacroSerializer());

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
    strictMapper = new ObjectMapper(builder.build());
    strictMapper.getSerializerProviderInstance().setNullKeySerializer(new NullKeySerializer());
    strictMapper.registerModule(module);

    //
    // Configure loose mapper
    //
    builder.disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    looseMapper = new ObjectMapper(builder.build());
    looseMapper.getSerializerProviderInstance().setNullKeySerializer(new NullKeySerializer());
    looseMapper.registerModule(module);
  }

  //
  // Method to deserialize JSON to Objects.
  //

  public static Object jsonToObject(String json) throws JsonProcessingException {
    return strictMapper.readValue(json, Object.class);
  }

  //
  // Methods to serialize objects to JSON
  //

  public static String objectToJson(Object o) throws IOException {
    return objectToJson(o, MAX_JSON_SIZE_DEFAULT);
  }

  public static String objectToJson(Object o, int maxJsonSize) throws IOException {
    return objectToJson(o, false, maxJsonSize);
  }

  public static String objectToJson(Object o, boolean isStrict) throws IOException {
    return objectToJson(o, isStrict, MAX_JSON_SIZE_DEFAULT);
  }

  public static String objectToJson(Object o, boolean isStrict, int maxJsonSize) throws IOException {
    StringWriter writer = new StringWriter();
    objectToJson(writer, o, isStrict, maxJsonSize);
    return writer.toString();
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict) throws IOException {
    objectToJson(writer, o, isStrict, MAX_JSON_SIZE_DEFAULT);
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict, int maxJsonSize) throws IOException {
    BoundedWriter boundedWriter = new BoundedWriter(writer, maxJsonSize);
    try {
      if (isStrict) {
        strictMapper.writeValue(boundedWriter, o);
      } else {
        looseMapper.writeValue(boundedWriter, o);
      }
    } catch (BoundedWriter.WriterBoundReachedException wbre) {
      throw new IOException("Resulting JSON is too big.", wbre);
    }
  }

}
