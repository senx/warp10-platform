//
//   Copyright 2018-2020  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.TreeSet;

/**
 * Unpack a GeoXPShape
 * 
 * We relay on GTSWrappers for this, this is kinda weird but hey, it works!
 * 
 */
public class GEOUNPACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GEOUNPACK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    byte[] serialized;
    
    if (o instanceof String) {
      serialized = OrderPreservingBase64.decode(o.toString().getBytes(StandardCharsets.US_ASCII));
    } else if (o instanceof byte[]) {
      serialized = (byte[]) o;
    } else {
      throw new WarpScriptException(getName() + " expects a packed shape on top of the stack.");      
    }
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    GTSWrapper wrapper = new GTSWrapper();
    
    try {
      deserializer.deserialize(wrapper, serialized);
    } catch (TException te) {
      throw new WarpScriptException(te);
    }
    
    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);

    // Use a TreeSet to make sure geocells are sorted and without duplicate.
    TreeSet<Long> cells = new TreeSet<Long>();

    int idx = 0;
    
    while(decoder.next()) {
      // We are only interested in the timestamp which is the cell
      long cell = decoder.getTimestamp();
      // Only add cells with valid resolution (1-15)
      if (0L != (cell & 0xf000000000000000L)) {
        cells.add(cell);
      }
    }
    
    GeoXPShape shape = GeoXPLib.fromCells(cells);
    
    stack.push(shape);
    
    return stack;
  }
}
