//
//   Copyright 2018-2022  SenX S.A.S.
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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Apply merge on GTS instances
 */
public class MERGE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MERGE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    Boolean reversed = null;

    if (top instanceof Boolean) {
      reversed = (Boolean) top;
      top = stack.pop();
    }

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a LIST containing GTS or ENCODERs.");
    }

    List<Object> params = (List<Object>) top;

    if (null != reversed) {
      try {
        stack.push(GTSHelper.sortedMerge(params, reversed));
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " encountered an error while merging.", wse);
      }
    } else {
      List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
      List<GTSEncoder> encoders = new ArrayList<GTSEncoder>();

      for (int i = 0; i < params.size(); i++) {
        if (params.get(i) instanceof GeoTimeSerie) {
          series.add((GeoTimeSerie) params.get(i));
        } else if (params.get(i) instanceof GTSEncoder) {
          encoders.add((GTSEncoder) params.get(i));
        } else if (params.get(i) instanceof List) {
          for (Object o: (List) params.get(i)) {
            if (o instanceof GeoTimeSerie) {
              series.add((GeoTimeSerie) o);
            } else if (o instanceof GTSEncoder) {
              encoders.add((GTSEncoder) o);
            } else {
              throw new WarpScriptException(getName() + " expects a list of Geo Time Series or encoders as first parameter.");
            }
          }
        }
      }

      if (!encoders.isEmpty() && !series.isEmpty()) {
        throw new WarpScriptException(getName() + " can only operate on homogeneous lists of Geo Time Series or encoders.");
      }

      try {
        if (encoders.isEmpty()) {
          GeoTimeSerie merged = GTSHelper.mergeViaEncoders(series);

          stack.push(merged);
        } else {
          GTSEncoder encoder = new GTSEncoder(0L);

          encoder.setMetadata(encoders.get(0).getMetadata());

          for (GTSEncoder enc: encoders) {
            encoder.merge(enc);
          }

          stack.push(encoder);
        }
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " failed.", ioe);
      }
    }

    return stack;
  }
}
