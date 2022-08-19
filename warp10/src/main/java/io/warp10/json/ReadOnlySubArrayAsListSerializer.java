//
//   Copyright 2022  SenX S.A.S.
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.warp10.continuum.gts.ReadOnlySubArrayAsList;

import java.io.IOException;

public class ReadOnlySubArrayAsListSerializer extends StdSerializer<ReadOnlySubArrayAsList> {

  protected ReadOnlySubArrayAsListSerializer() {
    super(ReadOnlySubArrayAsList.class);
  }

  @Override
  public void serialize(ReadOnlySubArrayAsList l, JsonGenerator gen, SerializerProvider provider) throws IOException {

    gen.writeStartArray(l.size());
    switch (l.elementDataType) {
      case LONG:
        for (int i = 0; i < l.size(); i++) {
          gen.writeNumber((long) l.get(i));
        }
        break;
      case DOUBLE:
        for (int i = 0; i < l.size(); i++) {
          gen.writeNumber((double) l.get(i));
        }
        break;
      case STRING:
        for (int i = 0; i < l.size(); i++) {
          gen.writeString((String) l.get(i));
        }
        break;
      case BOOLEAN:
        for (int i = 0; i < l.size(); i++) {
          gen.writeBoolean((boolean) l.get(i));
        }
        break;
    }
    gen.writeEndArray();
  }
}
