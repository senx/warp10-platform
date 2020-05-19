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

package io.warp10.continuum.ingress;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.ValueEncoder;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.warp.sdk.WarpScriptExtension;

public class MacroValueEncoder extends ValueEncoder {
  
  private static final String DEFAULT_PREFIX = ":m:";
  private static final String DEFAULT_MACRO_PREFIX ="";
  private static final String KEY_STACK = ":m:stack";
  
  private final String prefix;
  private final String macroPrefix;
  private final int prefixLen;
  
  public static final class Extension extends WarpScriptExtension {

    public Extension() {
      ValueEncoder.register(new MacroValueEncoder());
    }
    
    @Override
    public Map<String, Object> getFunctions() {
      return null;
    }
  }
  
  private static final Macro NOOP_MACRO = new Macro();
  
  public MacroValueEncoder() {
    this.prefix = WarpConfig.getProperty(Configuration.CONFIG_MACRO_VALUE_ENCODER_PREFIX, DEFAULT_PREFIX);
    this.prefixLen = this.prefix.length();
    String mprefix = WarpConfig.getProperty(Configuration.CONFIG_MACRO_VALUE_ENCODER_MACRO_PREFIX, DEFAULT_MACRO_PREFIX);
    if (!mprefix.isEmpty() && !mprefix.endsWith("/")) {
      this.macroPrefix = mprefix + "/";
    } else {
      this.macroPrefix = mprefix;
    }
  }
  
  @Override
  public Object parseValue(String value) throws Exception {
    if (!value.startsWith(prefix)) {
      return null;
    }
    
    // Extract the macro name, adding the specified prefix
    String macro = this.macroPrefix + value.substring(prefixLen, value.indexOf(':', prefixLen));

    MemoryWarpScriptStack stack = (MemoryWarpScriptStack) WarpConfig.getThreadProperty(KEY_STACK);
    
    if (null == stack) {
      stack = new MemoryWarpScriptStack(null, null);
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[MacroValueEncoder @" + macro + " " + Thread.currentThread().getName() + "]");      
      // Disable function metrics so execution is faster
      stack.setFunctionMetrics(false);
      WarpConfig.setThreadProperty(KEY_STACK, stack);
    }
    
    try {
      stack.push(value.substring(prefixLen + macro.length() + 1));
      stack.run(macro);
      return stack.pop();
    } finally {
      stack.clear();
    }
  }
}
