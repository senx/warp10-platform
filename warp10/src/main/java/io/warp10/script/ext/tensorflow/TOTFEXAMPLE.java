package io.warp10.script.ext.tensorflow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOTFEXAMPLE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOTFEXAMPLE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Map) && !(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a map or a serialized TensorExample on top of the stack.");
    }
    
    Example example = null;

    if (top instanceof Map) {
      Map<Object,Object> map = (Map<Object,Object>) top;
      
      Features.Builder features = Features.newBuilder();
      
      for (Entry<Object,Object> entry: map.entrySet()) {
        if (!(entry.getKey() instanceof String)) {
          throw new WarpScriptException(getName() + " expects map keys to be strings.");
        }
        
        Object value = entry.getValue();
        String key = entry.getKey().toString();
        
        if (value instanceof Long || value instanceof Integer || value instanceof Byte || value instanceof BigInteger) {
          Int64List il = Int64List.newBuilder().addValue(((Number) value).longValue()).build();
          features.putFeature(key, Feature.newBuilder().setInt64List(il).build());
        } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
          FloatList fl = FloatList.newBuilder().addValue(((Number) value).floatValue()).build();        
          features.putFeature(key, Feature.newBuilder().setFloatList(fl).build());
        } else if (value instanceof byte[]) {
          BytesList bl = BytesList.newBuilder().addValue(ByteString.copyFrom((byte[]) value)).build();
          features.putFeature(key, Feature.newBuilder().setBytesList(bl).build());
        } else if (value instanceof String) {
          BytesList bl = BytesList.newBuilder().addValue(ByteString.copyFrom((String) value, Charsets.UTF_8)).build();        
          features.putFeature(key, Feature.newBuilder().setBytesList(bl).build());
        } else if (value instanceof List) {
          List<Object> l = (List<Object>) value;
          
          boolean typed = false;
          
          Int64List.Builder ib = null;
          FloatList.Builder fb = null;
          BytesList.Builder bb = null;
          
          for (Object elt: l) {
            if (elt instanceof Long || elt instanceof Integer || elt instanceof Byte || elt instanceof BigInteger) {
              if (typed && null == ib) {
                throw new WarpScriptException(getName() + " expects value lists to contains elements of the same type.");
              }
              if (null == ib) {
                typed = true;
                ib = Int64List.newBuilder();
              }
              ib.addValue(((Number) elt).longValue());
            } else if (elt instanceof Double || elt instanceof Float || elt instanceof BigDecimal) {
              if (typed && null == fb) {
                throw new WarpScriptException(getName() + " expects value lists to contains elements of the same type.");
              }
              if (null == fb) {
                typed = true;
                fb = FloatList.newBuilder();
              }
              fb.addValue(((Number) elt).floatValue());
            } else if (elt instanceof byte[]) {
              if (typed && null == bb) {
                throw new WarpScriptException(getName() + " expects value lists to contains elements of the same type.");
              }
              if (null == bb) {
                typed = true;
                bb = BytesList.newBuilder();
              }
              bb.addValue(ByteString.copyFrom((byte[]) elt));
            } else if (elt instanceof String) {
              if (typed && null == bb) {
                throw new WarpScriptException(getName() + " expects value lists to contains elements of the same type.");
              }
              if (null == bb) {
                typed = true;
                bb = BytesList.newBuilder();
              }
              bb.addValue(ByteString.copyFrom((String) elt, Charsets.UTF_8));
            }
          }
          
          if (null != bb) {
            features.putFeature(key, Feature.newBuilder().setBytesList(bb).build());
          } else if (null != ib) {
            features.putFeature(key, Feature.newBuilder().setInt64List(ib).build());
          } else if (null != fb) {
            features.putFeature(key, Feature.newBuilder().setFloatList(fb).build());          
          } else {
            throw new WarpScriptException(getName() + " encountered an empty value list.");
          }                
        }
      }
      
      example = Example.newBuilder().setFeatures(features).build();      
    } else {
      try {
        example = Example.parseFrom((byte[]) top);
      } catch (InvalidProtocolBufferException ipbe) {
        throw new WarpScriptException(getName() + " encoutered an error while parsing TensorFlow Example.");
      }
    }

    stack.push(example);
    
    return stack;
  }
}
