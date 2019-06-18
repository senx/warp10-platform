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

package io.warp10.script.functions;

import com.google.common.base.Charsets;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Wrap a GTS into a GTSWrapper
 */
public class WRAP extends ElementOrListStackFunction {

  private final boolean opt;
  private final boolean compress;
  private final ElementStackFunction function;

  public WRAP(String name) {
    this(name, false);
  }

  public WRAP(String name, boolean opt) {
    super(name);
    this.opt = opt;
    this.compress = true;

    function = generateFunctionOnce();
  }

  public WRAP(String name, boolean opt, boolean compress) {
    super(name);
    this.opt = opt;
    this.compress = compress;

    if (this.opt && !this.compress) {
      throw new RuntimeException("Invalid combination of opt and compress.");
    }

    function = generateFunctionOnce();
  }

  private ElementStackFunction generateFunctionOnce() {
    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        GTSWrapper wrapper;
        if (element instanceof GeoTimeSerie) {
          if (opt) {
            wrapper = GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) element, compress, 1.0, true);
          } else {
            wrapper = GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) element, compress);
          }
        } else if (element instanceof GTSEncoder) {
          if (opt) {
            wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) element, compress, 1.0);
          } else {
            wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) element, compress);
          }
        } else {
          throw new WarpScriptException(getName() + " expects a Geo Time Series of a GTSEncoder or a list on top of the stack");
        }

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

        try {
          byte[] bytes = serializer.serialize(wrapper);

          return new String(OrderPreservingBase64.encode(bytes), Charsets.US_ASCII);
        } catch (TException te) {
          throw new WarpScriptException(getName() + " failed to wrap GTS.", te);
        }
      }
    };
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return function;
  }

}
