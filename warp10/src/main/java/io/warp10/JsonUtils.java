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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class JsonUtils {

  private static class NullKeySer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeFieldName("");
    }
  }

  private static final ObjectMapper strictMapper = new ObjectMapper();
  private static final ObjectMapper looseMapper = new ObjectMapper();

  static {
    strictMapper.enable(JsonGenerator.Feature.ESCAPE_NON_ASCII);
    looseMapper.enable(JsonGenerator.Feature.ESCAPE_NON_ASCII);

    looseMapper.getSerializerProvider().setNullKeySerializer(new NullKeySer());
    looseMapper.enable(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS);
    looseMapper.disable(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS);
  }

  public static String ObjectToJson(Object o, boolean isStrict) throws IOException {
    if (isStrict) {
      return strictMapper.writeValueAsString(o);
    } else {
      return looseMapper.writeValueAsString(o);
    }
  }

  public static Object JsonToObject(String json, boolean isStrict) throws IOException {
    if (isStrict) {
      return strictMapper.readValue(json, Object.class);
    } else {
      return looseMapper.readValue(json, Object.class);
    }
  }
}
