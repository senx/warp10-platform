//
//   Copyright 2023  SenX S.A.S.
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
import io.warp10.script.WarpScriptAuditStatement;

import java.io.IOException;

public class WarpScriptAuditStatementSerializer extends StdSerializer<WarpScriptAuditStatement> {


  protected WarpScriptAuditStatementSerializer() {
    super(WarpScriptAuditStatement.class);
  }

  @Override
  public void serialize(WarpScriptAuditStatement st, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(WarpScriptAuditStatement.KEY_TYPE,st.type.name());
    gen.writeNumberField(WarpScriptAuditStatement.KEY_LINE,st.lineNumber);
    gen.writeNumberField(WarpScriptAuditStatement.KEY_POSITION,st.positionNumber);
    gen.writeStringField(WarpScriptAuditStatement.KEY_STATEMENT,st.statement);
    gen.writeEndObject();
  }
}
