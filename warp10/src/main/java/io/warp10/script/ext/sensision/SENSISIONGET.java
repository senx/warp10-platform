package io.warp10.script.ext.sensision;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.sensision.Sensision;

import java.util.Map;

import com.geoxp.GeoXPLib;

/**
 * Retrieve a datapoint currently stored in Sensision
 */
public class SENSISIONGET extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SENSISIONGET(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    String cls;
    Map<String,String> labels;

    if (top instanceof String) {
      Object[] parsed = PARSESELECTOR.parse(top.toString());
    
      cls = parsed[0].toString();
      labels = (Map<String,String>) parsed[1];
    } else {
      labels = (Map<String,String>) top;
      cls = stack.pop().toString();
    }

    GeoTimeSerie gts = new GeoTimeSerie();
    gts.setName(cls);
    gts.setLabels(labels);

    Object value = Sensision.getValue(cls, labels);

    if (null != value) {
      long timestamp = Constants.TIME_UNITS_PER_MS * (Sensision.getTimestamp(cls, labels) / Sensision.TIME_UNITS_PER_MS);
      float[] latlon = Sensision.getLocation(cls, labels);
      Long elevation = Sensision.getElevation(cls, labels);
      
      long geoxppoint = null == latlon ? GeoTimeSerie.NO_LOCATION : GeoXPLib.toGeoXPPoint(latlon[0], latlon[1]);
      long elev = null == elevation ? GeoTimeSerie.NO_ELEVATION : elevation;
      GTSHelper.setValue(gts, timestamp, geoxppoint, elev, value, false);      
    }
    
    stack.push(gts);
        
    return stack;
  }
  
}
