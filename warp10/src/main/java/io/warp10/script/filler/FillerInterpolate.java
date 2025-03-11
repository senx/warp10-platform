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

package io.warp10.script.filler;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptSingleValueFillerFunction;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;

import java.util.ArrayList;

public class FillerInterpolate extends NamedWarpScriptFunction implements WarpScriptFillerFunction, WarpScriptSingleValueFillerFunction.Precomputable {
  
  public FillerInterpolate(String name) {
    super(name);
  }
  
  @Override
  public Object[] apply(Object[] args) throws WarpScriptException {
    
    Object[] results = new Object[4];
    
    Object[] prev = (Object[]) args[1];
    Object[] other = (Object[]) args[2];
    Object[] next = (Object[]) args[3];

    // We cannot interpolate on the edges
    if (null == prev[0] || null == next[0]) {
      return results;
    }

    long tick = ((Number) other[0]).longValue();
    
    long prevtick = ((Number) prev[0]).longValue();
    long prevloc = ((Number) prev[1]).longValue();
    long prevelev = ((Number) prev[2]).longValue();
    Object prevvalue = prev[3];

    long nexttick = ((Number) next[0]).longValue();
    long nextloc = ((Number) next[1]).longValue();
    long nextelev = ((Number) next[2]).longValue();
    Object nextvalue = next[3];

    // We cannot interpolate STRING or BOOLEAN values
    if (prevvalue instanceof String || prevvalue instanceof Boolean) {
      return results;
    }
    
    //
    // Compute the interpolated value
    //
    
    long span = nexttick - prevtick;
    long delta = tick - prevtick;
    double rate = (((Number) nextvalue).doubleValue() - ((Number) prevvalue).doubleValue())/span;
    
    double interpolated = ((Number) prevvalue).doubleValue() + rate * delta;
    
    if (prevvalue instanceof Long) {
      results[3] = (long) Math.round(interpolated);
    } else {
      results[3] = interpolated;
    }
    
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (GeoTimeSerie.NO_LOCATION != prevloc && GeoTimeSerie.NO_LOCATION != nextloc) {
      double[] prevlatlon = GeoXPLib.fromGeoXPPoint(prevloc);
      double[] nextlatlon = GeoXPLib.fromGeoXPPoint(nextloc);
      
      double lat = prevlatlon[0] + delta * ((nextlatlon[0] - prevlatlon[0]) / span);
      double lon = prevlatlon[1] + delta * ((nextlatlon[1] - prevlatlon[1]) / span);
      
      location = GeoXPLib.toGeoXPPoint(lat, lon);
    }
    
    if (GeoTimeSerie.NO_ELEVATION != prevelev && GeoTimeSerie.NO_ELEVATION != nextelev) {
      elevation = (long) Math.round(prevelev + delta * ((nextelev - prevelev) / (double) span));
    }
    
    results[0] = tick;
    results[1] = location;
    results[2] = elevation;
    
    return results;
  }
  
  @Override
  public int getPreWindow() {
    return 1;
  }
  
  @Override
  public int getPostWindow() {
    return 1;
  }
  
  public WarpScriptSingleValueFillerFunction compute(GeoTimeSerie gts) throws WarpScriptException {
    
    final PolynomialSplineFunction valFunction;
    final PolynomialSplineFunction latFunction;
    final PolynomialSplineFunction lonFunction;
    final PolynomialSplineFunction elevationFunction;

    if (gts.size() == 0) {
      valFunction = null;
      latFunction = null;
      lonFunction = null;
      elevationFunction = null;
    } else {
      if (GeoTimeSerie.TYPE.DOUBLE != gts.getType() && GeoTimeSerie.TYPE.LONG != gts.getType()) {
        throw new WarpScriptException(getName() + " expects a GTS of type DOUBLE or LONG, but instead got a GTS of type " + gts.getType().name());
      }

      // values
      double[] xval = GTSHelper.getTicksAsDouble(gts);
      double[] fval = GTSHelper.getValuesAsDouble(gts);

      int size = gts.size();
      if (size > 1) {
        valFunction = (new LinearInterpolator()).interpolate(xval, fval);
      } else {
        valFunction = null;
      }

      // positions
      // how many ticks do have a location ?
      int nLoc = 0;
      for (int i = 0; i < gts.size(); i++) {
        if (GeoTimeSerie.NO_LOCATION != GTSHelper.locationAtIndex(gts, i)) {
          nLoc++;
        }
      }
      if (nLoc > 0) {
        double[] xLocTicks = new double[nLoc];
        double[] fLatVal = new double[nLoc];
        double[] fLonVal = new double[nLoc];
        int idx = 0;
        for (int i = 0; i < gts.size(); i++) {
          long l = GTSHelper.locationAtIndex(gts, i);
          if (l != GeoTimeSerie.NO_LOCATION) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(l);
            xLocTicks[idx] = GTSHelper.tickAtIndex(gts, i);
            fLatVal[idx] = latlon[0];
            fLonVal[idx] = latlon[1];
            idx++;
          }
        }
        latFunction = (new LinearInterpolator()).interpolate(xLocTicks, fLatVal);
        lonFunction = (new LinearInterpolator()).interpolate(xLocTicks, fLonVal);
      } else {
        latFunction = null;
        lonFunction = null;
      }


      // elevations
      // how many ticks do have an elevation ?
      int nElev = 0;
      for (int i = 0; i < gts.size(); i++) {
        if (GeoTimeSerie.NO_ELEVATION != GTSHelper.elevationAtIndex(gts, i)) {
          nElev++;
        }
      }
      if (nElev > 0) {
        double[] xElevTicks = new double[nElev];
        double[] fElevVal = new double[nElev];
        int idx = 0;
        for (int i = 0; i < gts.size(); i++) {
          long e = GTSHelper.elevationAtIndex(gts, i);
          if (e != GeoTimeSerie.NO_ELEVATION) {
            xElevTicks[idx] = GTSHelper.tickAtIndex(gts, i);
            fElevVal[idx] = e;
            idx++;
          }
        }
        elevationFunction = (new LinearInterpolator()).interpolate(xElevTicks, fElevVal);
      } else {
        elevationFunction = null;
      }
    }
    


    return new WarpScriptSingleValueFillerFunction() {
      @Override
      public void fillTick(long tick, GeoTimeSerie gts, Object invalidValue) throws WarpScriptException {
        long position = GeoTimeSerie.NO_LOCATION;
        long elevation = GeoTimeSerie.NO_ELEVATION;
        if (null != elevationFunction && elevationFunction.isValidPoint(tick)) {
          elevation = Math.round(elevationFunction.value(tick));
        }
        if (null != latFunction && latFunction.isValidPoint(tick)) {
          position = GeoXPLib.toGeoXPPoint(latFunction.value(tick), lonFunction.value(tick));
        }
        if (null != valFunction && valFunction.isValidPoint(tick)) {
          GTSHelper.setValue(gts, tick, position, elevation, valFunction.value(tick), false);
        } else if (null != invalidValue) {
          GTSHelper.setValue(gts, tick, position, elevation, invalidValue, false);
        }
      }
    };
  }
}
