package io.warp10.script.ext.tensorflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TFEXAMPLETO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TFEXAMPLETO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Example)) {
      throw new WarpScriptException(getName() + " expects a TensorFlow Example on top of the stack.");
    }

    Map<String,Object> map = new HashMap<String,Object>();
      
    Features features = ((Example) top).getFeatures();
    Map<String,Feature> featmap = features.getFeatureMap();
      
    for (Entry<String,Feature> entry: featmap.entrySet()) {
      List<Object> values = new ArrayList<Object>();
      map.put(entry.getKey(), values);
              
      Feature feature = entry.getValue();
      
      BytesList bl = entry.getValue().getBytesList();
      if (bl.getValueCount() > 0) {
        for (ByteString bs: bl.getValueList()) {
          values.add(bs.toByteArray());
        }
        continue;
      }
      
      Int64List il = entry.getValue().getInt64List();
      if (il.getValueCount() > 0) {
        for (Long l: il.getValueList()) {
          values.add(l);
        }
        continue;
      }
      
      FloatList fl = entry.getValue().getFloatList();
      if (fl.getValueCount() > 0) {
        for (Float f: fl.getValueList()) {
          values.add(f.doubleValue());
        }
      }        
    }

    stack.push(map);
    
    return stack;
  }
}
