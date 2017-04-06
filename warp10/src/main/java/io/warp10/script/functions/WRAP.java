//
//   Copyright 2016  Cityzen Data
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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

/**
 * Wrap a GTS into a GTSWrapper
 */
public class WRAP  extends GTSStackFunction {
  
  private final boolean opt;
  
  public WRAP(String name) {
    this(name, false);
  }
  
  public WRAP(String name, boolean opt) {
    super(name);
    this.opt = opt;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (!(stack.peek() instanceof GTSEncoder)) {
      return super.apply(stack);      
    }
    
    GTSEncoder encoder = (GTSEncoder) stack.pop();
    
    GTSWrapper wrapper;
    
    if (opt) {
      wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true, 1.0);
    } else {
      wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
    }
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    try {
      byte[] bytes = serializer.serialize(wrapper);
      
      stack.push(new String(OrderPreservingBase64.encode(bytes), Charsets.US_ASCII));
    } catch (TException te) {
      throw new WarpScriptException(getName() + " failed to wrap GTS.");
    }        

    return stack;
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    return null;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {

    GTSWrapper wrapper;
    
    if (opt) {
      wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gts, true, 1.0, true);
    } else {
      wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gts, true);
    }
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    try {
      byte[] bytes = serializer.serialize(wrapper);
      
      return new String(OrderPreservingBase64.encode(bytes), Charsets.US_ASCII);
    } catch (TException te) {
      throw new WarpScriptException(getName() + " failed to wrap GTS.");
    }        
  }
}
