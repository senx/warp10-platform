//
//   Copyright 2021  SenX S.A.S.
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
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Merge sorted GTS or encoders, leading to a sorted result
 */
public class SMERGE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private class GTSAndIndex {
    private GeoTimeSerie gts;
    private int idx;
  }

  public SMERGE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    boolean reversed = false;

    if (top instanceof Boolean) {
      reversed = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input.");
    }

    List<Object> params = (List<Object>) top;

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
    List<GTSEncoder> encoders = new ArrayList<GTSEncoder>();

    for (int i = 0; i < params.size(); i++) {
      if (params.get(i) instanceof GeoTimeSerie) {
        if (!GTSHelper.isSorted((GeoTimeSerie) params.get(i))) {
          throw new WarpScriptException(getName() + " GTS " + GTSHelper.buildSelector((GeoTimeSerie) params.get(i), false) + " is not sorted.");
        }
        if (GTSHelper.isReversed((GeoTimeSerie) params.get(i)) != reversed) {
          throw  new WarpScriptException(getName() + " GTS " + GTSHelper.buildSelector((GeoTimeSerie) params.get(i), false) + " is not sorted in the expected order.");
        }
        series.add((GeoTimeSerie) params.get(i));
      } else if (params.get(i) instanceof GTSEncoder) {
        encoders.add((GTSEncoder) params.get(i));
      } else if (params.get(i) instanceof List) {
        for (Object o: (List) params.get(i)) {
          if (o instanceof GeoTimeSerie) {
            if (!GTSHelper.isSorted((GeoTimeSerie) o)) {
              throw new WarpScriptException(getName() + " GTS " + GTSHelper.buildSelector((GeoTimeSerie) o, false) + " is not sorted.");
            }
            if (GTSHelper.isReversed((GeoTimeSerie) o) != reversed) {
              throw  new WarpScriptException(getName() + " GTS " + GTSHelper.buildSelector((GeoTimeSerie) o, false) + " is not sorted in the expected order.");
            }
            series.add((GeoTimeSerie) o);
          } else if (o instanceof GTSEncoder) {
            encoders.add((GTSEncoder) o);
          } else {
            throw new WarpScriptException(getName() + " expects a list of Geo Time Series or encoders or of lists therefos.");
          }
        }
      }
    }

    // Index at which the GTS start
    int gtsidx = encoders.size();
    BitSet eligible = new BitSet(encoders.size() + series.size());
    // Decoders used to scan the ENCODERS
    final boolean freversed = reversed;
    PriorityQueue<Object> decoders = new PriorityQueue<Object>(new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
        Long ts1 = null;
        Long ts2 = null;

        if (o1 instanceof GTSDecoder) {
          ts1 = ((GTSDecoder) o1).getTimestamp();
        } else { // GTSAndIndex
          GTSAndIndex gtsi = (GTSAndIndex) o1;
          if (gtsi.idx < gtsi.gts.size()) {
            ts1 = GTSHelper.tickAtIndex(gtsi.gts, gtsi.idx);
          }
        }

        if (o2 instanceof GTSDecoder) {
          ts2 = ((GTSDecoder) o2).getTimestamp();
        } else { // GTSAndIndex
          GTSAndIndex gtsi = (GTSAndIndex) o2;
          if (gtsi.idx < gtsi.gts.size()) {
            ts2 = GTSHelper.tickAtIndex(gtsi.gts, gtsi.idx);
          }
        }

        if (null == ts1 && null == ts2) {
          return 0;
        } else if (null == ts1) {
          if (freversed) {
            return -1;
          } else {
            return 1;
          }
        } else if (null == ts2) {
          if (freversed) {
            return 1;
          } else {
            return -1;
          }
        } else {
          if (!freversed) {
            return Long.compare(ts1, ts2);
          } else {
            return Long.compare(ts2, ts1);
          }
        }
      }
    });

    // FIXME, use a TreeMap ordered by timestamp so we can simply iterate without having to go over all
    // encoders

    for (int i = 0; i < encoders.size(); i++) {
      GTSDecoder decoder = encoders.get(i).getDecoder(true);
      // Nullify decoders[i] if the decoder is exhausted
      if (decoder.next()) {
        decoders.add(decoder);
      }
    }

    for (int i = 0; i < series.size(); i++) {
      if (0 == series.get(i).size()) {
        continue;
      }
      GTSAndIndex gtsi = new GTSAndIndex();
      gtsi.gts = series.get(i);
      gtsi.idx = 0;
      decoders.add(gtsi);
    }

    GTSEncoder merged = new GTSEncoder(0);

    if (!encoders.isEmpty()) {
      merged.setMetadata(encoders.get(0).getMetadata());
    } else if (!series.isEmpty()) {
      merged.setMetadata(series.get(0).getMetadata());
    }

    try {
      while(!decoders.isEmpty()) {
        Object first = decoders.peek();
        Long ts = null;
        Long lastTs = null;

        if (first instanceof GTSDecoder) {
          GTSDecoder decoder = (GTSDecoder) first;
          lastTs = decoder.getTimestamp();
        } else {
          GTSAndIndex gtsi = (GTSAndIndex) first;
          if (gtsi.idx < gtsi.gts.size()) {
            lastTs = GTSHelper.tickAtIndex(gtsi.gts, gtsi.idx);
          }
        }
        if (null == lastTs) {
          break;
        }

        boolean done = false;
        // Iterate over the elements of the Queue
        while(!decoders.isEmpty() && !done) {
          Object elt = decoders.remove();
          if (elt instanceof GTSDecoder) {
            GTSDecoder decoder = (GTSDecoder) elt;
            ts = decoder.getTimestamp();
            if (!lastTs.equals(ts)) { // The first timestamp does not equal lastTs, so we are done for this loop
              decoders.add(elt);
              done = true;
              continue;
            }
            boolean exhausted = false;
            while(lastTs.equals(ts)) {
              merged.addValue(ts, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
              if (decoder.next()) {
                ts = decoder.getTimestamp();
                // Ensure the ENCODER is sorted correctly
                if (reversed && ts > lastTs) {
                  throw new WarpScriptException(getName() + " ENCODER " + GTSHelper.buildSelector(decoder.getMetadata(), false) + " is not sorted in expected order.");
                } else if (!reversed && ts < lastTs) {
                  throw new WarpScriptException(getName() + " ENCODER " + GTSHelper.buildSelector(decoder.getMetadata(), false) + " is not sorted in expected order.");
                }
              } else {
                ts = null;
                exhausted = true;
              }
            }
            if (!exhausted) {
              decoders.add(decoder);
            }
          } else { // GTSAndIndex
            GTSAndIndex gtsi = (GTSAndIndex) elt;
            ts = GTSHelper.tickAtIndex(gtsi.gts, gtsi.idx);
            if (!lastTs.equals(ts)) {
              decoders.add(elt);
              done = true;
              continue;
            }
            while(lastTs.equals(ts)) {
              merged.addValue(ts, GTSHelper.locationAtIndex(gtsi.gts, gtsi.idx), GTSHelper.elevationAtIndex(gtsi.gts, gtsi.idx), GTSHelper.valueAtIndex(gtsi.gts, gtsi.idx));
              gtsi.idx++;
              if (gtsi.idx >= gtsi.gts.size()) {
                break;
              }
              ts = GTSHelper.tickAtIndex(gtsi.gts, gtsi.idx);
            }
            if (gtsi.idx < gtsi.gts.size()) {
              decoders.add(gtsi);
            }
          }
        }
      }
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " encountered an error while merging.");
    }

    stack.push(merged);

    return stack;
  }
}
