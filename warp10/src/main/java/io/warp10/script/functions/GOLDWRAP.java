package io.warp10.script.functions;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Charsets;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.ElementOrListStackFunction.ElementStackFunction;

public class GOLDWRAP extends ElementOrListStackFunction {
  
  private final ElementStackFunction function;
  
  
  public GOLDWRAP(String name) {
    super(name);
    function = generateFunctionOnce();
  }

  private ElementStackFunction generateFunctionOnce() {
    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        GTSEncoder encoder = null;
        
        try {
          if (element instanceof GeoTimeSerie) {
            encoder = new GTSEncoder(0L);
            encoder.encode((GeoTimeSerie) element);
          } else if (element instanceof GTSEncoder) {
            encoder = (GTSEncoder) element;
          } else if (element instanceof String || element instanceof byte[]) {
            TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
            byte[] bytes;
            
            if (element instanceof String) {
              bytes = OrderPreservingBase64.decode(element.toString().getBytes(Charsets.US_ASCII));
            } else {
              bytes = (byte[]) element;
            }
            GTSWrapper wrapper = new GTSWrapper();
            deser.deserialize(wrapper, bytes);
            encoder = GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper);      
          } else {
            throw new WarpScriptException(getName() + " can only be applied to Geo Time Series™, GTS Encoders or wrapped instances of those types.");
          } 
          
          //
          // Split the encoder in 5 GTS, one per type, in this order:
          //
          // LONG, DOUBLE, BOOLEAN, STRING, BINARY
          //
          
          GeoTimeSerie[] gts = new GeoTimeSerie[5];

          for (int i = 0; i < gts.length; i++) {
            gts[i] = new GeoTimeSerie();
          }
          
          GTSDecoder decoder = encoder.getDecoder();
          
          // Populate the 5 GTS
          while(decoder.next()) {
            long ts = decoder.getTimestamp();
            long location = decoder.getLocation();
            long elevation = decoder.getElevation();
            Object value = decoder.getBinaryValue();
            
            if (value instanceof Long) {
              GTSHelper.setValue(gts[0], ts, location, elevation, value, false);
            } else if (value instanceof Double || value instanceof BigDecimal) {
              GTSHelper.setValue(gts[1], ts, location, elevation, value, false);          
            } else if (value instanceof Boolean) {
              GTSHelper.setValue(gts[2], ts, location, elevation, value, false);
            } else if (value instanceof String) {
              GTSHelper.setValue(gts[3], ts, location, elevation, value, false);
            } else if (value instanceof byte[]) {
              GTSHelper.setValue(gts[4], ts, location, elevation, value, false);
            }
          }
          
          // Sort the 5 GTS using fullsort so we get a deterministic order
          // in the presence of duplicate ticks
          
          for (int i = 0; i < gts.length; i++) {
            GTSHelper.fullsort(gts[i], false);
          }
          
          // Now merge the GTS in time order with the type precedence of the 'gts' array
          
          GTSEncoder enc = new GTSEncoder(0L);
          enc.setMetadata(encoder.getMetadata());
          
          int[] idx = new int[gts.length];
          
          while (true) {
            // Determine the next GTS to add from its timestamp, lowest first
            int gtsidx = -1;
            
            long ts = Long.MAX_VALUE;
            for (int i = 0; i < gts.length; i++) {
              if (idx[i] >= GTSHelper.nvalues(gts[i])) {
                continue;
              }
              long tick = GTSHelper.tickAtIndex(gts[i], idx[i]);
              if (tick < ts) {
                gtsidx = i;
                ts = tick;
              }
            }
            
            if (-1 == gtsidx) {
              break;
            }
            
            long tick = ts;
            
            do {
              long location = GTSHelper.locationAtIndex(gts[gtsidx], idx[gtsidx]);
              long elevation = GTSHelper.elevationAtIndex(gts[gtsidx], idx[gtsidx]);
              Object value = GTSHelper.valueAtIndex(gts[gtsidx], idx[gtsidx]);
              
              if (4 == gtsidx) { // BINARY
                value = value.toString().getBytes(Charsets.ISO_8859_1);
              } else if (2 == gtsidx) { // DOUBLE
                // Attempt to optimize the value
                value = GTSEncoder.optimizeValue(value);
              }
              
              enc.addValue(ts, location, elevation, value);
              
              idx[gtsidx]++;
            } while(idx[gtsidx] < GTSHelper.nvalues(gts[gtsidx]) && GTSHelper.tickAtIndex(gts[gtsidx], idx[gtsidx]) == ts);            
          }         
          GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(enc, true, 1.0D, Integer.MAX_VALUE);
          TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
          byte[] bytes = ser.serialize(wrapper);
          
          return bytes;
        } catch (TException te) {
          throw new WarpScriptException(getName() + " encountered an error while deserializing GTS Wrapper.", te);
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " encountered an error while deserializing GTS Wrapper.", ioe);
        }
      }
    };
  }
  
  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {        
    return function;
  }
}
