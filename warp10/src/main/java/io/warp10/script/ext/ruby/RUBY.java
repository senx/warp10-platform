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

package io.warp10.script.ext.ruby;

import io.warp10.script.functions.SCRIPTENGINE;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import org.jruby.embed.jsr223.JRubyEngineFactory;

public class RUBY extends SCRIPTENGINE {
  
  public RUBY(String name) {
    super(name, "ruby");
  }
  
  @Override
  protected ScriptEngine getEngine() {
    ScriptEngine se = new  JRubyEngineFactory().getScriptEngine();
    return se;
  }
  
  @Override
  protected Bindings getBindings(ScriptEngine engine) {
    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    return bindings;
  }
}
