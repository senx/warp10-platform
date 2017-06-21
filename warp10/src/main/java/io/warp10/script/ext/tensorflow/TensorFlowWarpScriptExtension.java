package io.warp10.script.ext.tensorflow;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class TensorFlowWarpScriptExtension extends WarpScriptExtension {
  
  private static Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();   
    
    functions.put("->TFEXAMPLE",  new TOTFEXAMPLE("->TFEXAMPLE"));
    functions.put("->TFRECORD",  new TOTFRECORD("->TFRECORD"));
    functions.put("->TFRECORDCRC32",  new TOTFRECORD("->TFRECORD", true));
    functions.put("TFRECORDCRC32->",  new TFRECORDTO("TFRECORD->", true));
    functions.put("TFRECORD->",  new TFRECORDTO("TFRECORD->"));
    functions.put("TFEXAMPLE->",  new TFEXAMPLETO("TFEXAMPLE->"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }  
}
