//
//   Copyright 2018-2021  SenX S.A.S.
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

import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Build a function which will compute a polynomial function
 */
public class POLYFUNC extends NamedWarpScriptFunction implements WarpScriptStackFunction, WarpScriptMapperFunction {

  private final PolynomialFunction func;

  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    public Builder(String name) {
      super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object top = stack.pop();

      if (!(top instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of polynomial coefficients on top of the stack.");
      }

      List<Object> l = (List<Object>) top;

      double[] coeffs = new double[l.size()];

      int i = 0;

      for (Object o: l) {
        if (!(o instanceof Number)) {
          throw new WarpScriptException(getName() + " expects polynomial coefficients to be numerical.");
        }
        coeffs[i++] = ((Number) o).doubleValue();
      }

      stack.push(new POLYFUNC(getName(), coeffs));

      return stack;
    }
  }

  public POLYFUNC(String name, double[] coeffs) {
    super(name);
    this.func = new PolynomialFunction(coeffs);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    boolean useBucketId = false;

    if (top instanceof Boolean) {
      useBucketId = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    if (useBucketId && (!(top instanceof GeoTimeSerie) || !GTSHelper.isBucketized((GeoTimeSerie) top))) {
      throw new WarpScriptException(getName() + " can only be applied to bucketized numerical Geo Time Series.");
    }

    if (top instanceof List) {
      List<Object> list = new ArrayList<Object>(((List) top).size());
      for (Object elt: (List) top) {
        if (!(elt instanceof Number)) {
          throw new WarpScriptException(getName() + " can only be applied to numerical values.");
        }
        list.add(this.func.value(((Number) elt).doubleValue()));
      }
      stack.push(list);
    } else if (top instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) top;
      int n = GTSHelper.nticks(gts);
      GeoTimeSerie out = new GeoTimeSerie(n);
      if (GTSHelper.isBucketized(gts)) {
        long lastbucket = GTSHelper.getLastBucket(gts);
        long bucketspan = GTSHelper.getBucketSpan(gts);
        for (int i = 0; i < n; i++) {
          long ts = useBucketId ? n - 1 - i : lastbucket - i * bucketspan;
          double value = this.func.value(ts);
          GTSHelper.setValue(out, ts, GTSHelper.locationAtTick(gts, ts), GTSHelper.elevationAtTick(gts, ts), value, false);
        }
      } else {
        for (int i = 0; i < n; i++) {
          long ts = GTSHelper.tickAtIndex(gts, i);
          double value = this.func.value(ts);
          GTSHelper.setValue(out, ts, GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
        }
      }
      stack.push(out);
    } else if (top instanceof GTSEncoder) {
      GTSDecoder decoder = ((GTSEncoder) top).getDecoder();
      GTSEncoder encoder = new GTSEncoder(decoder.getBaseTimestamp());
      encoder.setMetadata(decoder.getMetadata());

      while(decoder.next()) {
        long ts = decoder.getTimestamp();
        double value = this.func.value(ts);
        try {
          encoder.addValue(ts, decoder.getLocation(), decoder.getElevation(), value);
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " error while generating ENCODER.");
        }
      }
      stack.push(encoder);
    } else if (top instanceof Number) {
      double value = this.func.value(((Number) top).doubleValue());

      stack.push(value);
    } else {
      throw new WarpScriptException(getName() + " can only be applied to numerical values which may appear in LISTs, GTS or ENCODERs.");
    }


    return stack;
  }

  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    long tick = (long) args[0];
    long[] locations = (long[]) args[4];
    long[] elevations = (long[]) args[5];
    Object[] values = (Object[]) args[6];

    if (0 == values.length) {
      return new Object[] { tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, this.func.value(tick) };
    }

    if (1 != values.length) {
      throw new WarpScriptException(getName() + " can only be applied to a single value.");
    }

    return new Object[] { tick, locations[0], elevations[0], this.func.value(tick) };
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      List<Object> l = new ArrayList<Object>(this.func.getCoefficients().length);
      for(double d: this.func.getCoefficients()) {
        l.add(d);
      }
      SNAPSHOT.addElement(sb, l);
    } catch (WarpScriptException wse) {
      throw new RuntimeException("Error building coefficient snapshot", wse);
    }
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
