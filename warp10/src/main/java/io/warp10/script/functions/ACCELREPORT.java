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

package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.AcceleratorConfig;

public class ACCELREPORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String KEY_ACCELERATED = "accelerated";
  private static final String KEY_STATUS = "status";
  private static final String KEY_CACHE = "cache";
  private static final String KEY_PERSIST = "persist";
  private static final String KEY_CHUNK_COUNT = "chunkcount";
  private static final String KEY_CHUNK_SPAN = "chunkspan";
  
  private static final String KEY_DEFAULTS_WRITE = "defaults.write";
  private static final String KEY_DEFAULTS_DELETE = "defaults.delete";
  private static final String KEY_DEFAULTS_READ = "defaults.read";
  
  public ACCELREPORT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    LinkedHashMap<Object,Object> report = new LinkedHashMap<>();

    report.put(KEY_STATUS, AcceleratorConfig.isInstantiated());

    report.put(KEY_CACHE, AcceleratorConfig.isCache());
    report.put(KEY_PERSIST, AcceleratorConfig.isPersist());

    report.put(KEY_ACCELERATED, AcceleratorConfig.accelerated());

    report.put(KEY_CHUNK_COUNT, (long) AcceleratorConfig.getChunkCount());
    report.put(KEY_CHUNK_SPAN, AcceleratorConfig.getChunkSpan());

    List<String> defaults = new ArrayList<String>(2);
    if (AcceleratorConfig.getDefaultReadNocache()) {
      defaults.add(AcceleratorConfig.NOCACHE);
    } else {
      defaults.add(AcceleratorConfig.CACHE);
    }
    if (AcceleratorConfig.getDefaultReadNopersist()) {
      defaults.add(AcceleratorConfig.NOPERSIST);
    } else {
      defaults.add(AcceleratorConfig.PERSIST);
    }
    report.put(KEY_DEFAULTS_READ, defaults);

    defaults = new ArrayList<String>(2);
    if (AcceleratorConfig.getDefaultWriteNocache()) {
      defaults.add(AcceleratorConfig.NOCACHE);
    } else {
      defaults.add(AcceleratorConfig.CACHE);      
    }
    if (AcceleratorConfig.getDefaultWriteNopersist()) {
      defaults.add(AcceleratorConfig.NOPERSIST);
    } else {
      defaults.add(AcceleratorConfig.PERSIST);            
    }
    report.put(KEY_DEFAULTS_WRITE, defaults);
    
    defaults = new ArrayList<String>(2);
    if (AcceleratorConfig.getDefaultDeleteNocache()) {
      defaults.add(AcceleratorConfig.NOCACHE);
    } else {
      defaults.add(AcceleratorConfig.CACHE);      
    }
    if (AcceleratorConfig.getDefaultDeleteNopersist()) {
      defaults.add(AcceleratorConfig.NOPERSIST);
    } else {
      defaults.add(AcceleratorConfig.PERSIST);            
    }
    report.put(KEY_DEFAULTS_DELETE, defaults);
    
    stack.push(report);

    return stack;
  }

}
