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

package io.warp10.script.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.ElementOrListStackFunction.ElementStackFunction;

public class MVVALUES extends ElementOrListStackFunction implements ElementStackFunction {
  public MVVALUES(String name) {
    super(name);
  }
  
  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return this;
  }
  
  @Override
  public Object applyOnElement(Object element) throws WarpScriptException {
    if (!(element instanceof GTSEncoder) && !(element instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " can only be applied on Geo Time Series™ or GTS Encoders.");
    }
    
    return mvvalues(element);    
  }
  
  private static List<Object> mvvalues(Object element) throws WarpScriptException {
    List<Object> values = new ArrayList<Object>();

    GTSDecoder decoder = null;
    int nvalues = 0;
    
    if (element instanceof GTSEncoder) {
      decoder = ((GTSEncoder) element).getDecoder();
    } else {
      nvalues = GTSHelper.nvalues((GeoTimeSerie) element);
    }
    
    int idx = 0;
    
    boolean done = !(null == decoder ? idx < nvalues : decoder.next());
    
    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
    GTSWrapper wrapper = new GTSWrapper();
    
    while(!done) {
      Object value = null;
      
      if (null != decoder) {
        value = decoder.getBinaryValue();
        done = !decoder.next();
      } else {
        value = GTSHelper.valueAtIndex((GeoTimeSerie) element, idx);
        idx++;
        done = idx >= nvalues;
      }
      
      if (value instanceof Long || value instanceof Double || value instanceof Boolean) {
        values.add(value);
      } else if (value instanceof byte[]) {
        try {
          deser.deserialize(wrapper, (byte[]) value);
          values.add(mvvalues(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper)));
        } catch (IOException e) {
          throw new WarpScriptException("Error decoding.");
        } catch (TException te) {
          values.add(value);
        }
      } else if (value instanceof String) {
        if (null != decoder) {
          // We are getting values from a decoder, so a STRING is not a binary value
          values.add(value);
        } else {
          // Attempt to decode a Wrapper
          try {
            byte[] bytes = value.toString().getBytes(Charsets.ISO_8859_1);
            deser.deserialize(wrapper, bytes);
            values.add(mvvalues(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper)));
          } catch (IOException e) {
            throw new WarpScriptException("Error decoding.");
          } catch (TException te) {
            values.add(value);
          }
        }
      }
    }
    
    return values;
  }
}
