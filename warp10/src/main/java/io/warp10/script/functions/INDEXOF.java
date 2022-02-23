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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class INDEXOF extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public INDEXOF(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a LONG number of indexes to return. 0 to return all indexes.");
    }

    long numberOfIndexes = ((Long) o).longValue();

    // Non-strictly positive values means all indexes.
    if (numberOfIndexes <= 0) {
      numberOfIndexes = Long.MAX_VALUE;
    }

    Object objectToLookFor = stack.pop();

    Object coll = stack.pop();

    ArrayList<Object> indexes = new ArrayList<Object>();

    if (coll instanceof List) {
      List<Object> list = (List) coll;

      for (int i = 0; i < list.size(); i++) {
        Object element = list.get(i);
        if (Objects.equals(objectToLookFor, element)) {
          indexes.add((long) i);
          if (indexes.size() >= numberOfIndexes) {
            break;
          }
        }
      }
    } else if (coll instanceof Map) {
      Map<Object, Object> map = (Map) coll;
      for (Map.Entry<Object, Object> entry: map.entrySet()) {
        Object element = entry.getValue();
        if (Objects.equals(objectToLookFor, element)) {
          indexes.add(entry.getKey());
          if (indexes.size() >= numberOfIndexes) {
            break;
          }
        }
      }
    } else if (coll instanceof String) {
      if (!(objectToLookFor instanceof String)) {
        throw new WarpScriptException(getName() + " must be given a STRING to look for in a STRING");
      }
      String str = (String) coll;
      String strToLookFor = (String) objectToLookFor;

      for (int i = 0; i <= str.length() - strToLookFor.length(); i++) {
        boolean equals = true;

        for (int j = 0; j < strToLookFor.length(); j++) {
          if (str.charAt(i + j) != strToLookFor.charAt(j)) {
            equals = false;
            break;
          }
        }

        if (equals) {
          indexes.add((long) i);
          if (indexes.size() >= numberOfIndexes) {
            break;
          }
        }
      }
    } else if (coll instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) coll;

      GeoTimeSerie.TYPE gtsType = gts.getType();

      // If the GTS is numerical, make sure objectToLookFor is of the same type.
      if (gtsType == GeoTimeSerie.TYPE.LONG && objectToLookFor instanceof Double) {
        objectToLookFor = ((Number) objectToLookFor).longValue();
      } else if (gtsType == GeoTimeSerie.TYPE.DOUBLE && objectToLookFor instanceof Long) {
        objectToLookFor = ((Number) objectToLookFor).doubleValue();
      }

      // Check GTS and objectToLookFor are of compatible types
      if ((gtsType == GeoTimeSerie.TYPE.BOOLEAN && objectToLookFor instanceof Boolean)
          || (gtsType == GeoTimeSerie.TYPE.LONG && objectToLookFor instanceof Long)
          || (gtsType == GeoTimeSerie.TYPE.DOUBLE && objectToLookFor instanceof Double)
          || (gtsType == GeoTimeSerie.TYPE.STRING && objectToLookFor instanceof String)
      ) {
        for (int i = 0; i < gts.size(); i++) {
          Object element = GTSHelper.valueAtIndex(gts, i);
          if (objectToLookFor.equals(element)) {
            indexes.add((long) i);
            if (indexes.size() >= numberOfIndexes) {
              break;
            }
          }
        }
      }
    } else if (coll instanceof byte[]) {
      // Accept a Long representing a single byte as parameter, like GET and REMOVE output.
      if (objectToLookFor instanceof Long) {
        if (!objectToLookFor.equals(((Long) objectToLookFor) & 0xFFL)) {
          throw new WarpScriptException(getName() + " expects the LONG to look for to have only its least significant byte set when looking into BYTES.");
        }

        objectToLookFor = new byte[] {((Number) objectToLookFor).byteValue()};
      }

      if (!(objectToLookFor instanceof byte[])) {
        throw new WarpScriptException(getName() + " must be given a BYTES to look for in a BYTES.");
      }

      byte[] bytes = (byte[]) coll;
      byte[] bytesToLookFor = (byte[]) objectToLookFor;

      for (int i = 0; i <= bytes.length - bytesToLookFor.length; i++) {
        boolean equals = true;

        for (int j = 0; j < bytesToLookFor.length; j++) {
          if (bytes[i + j] != bytesToLookFor[j]) {
            equals = false;
            break;
          }
        }

        if (equals) {
          indexes.add((long) i);
          if (indexes.size() >= numberOfIndexes) {
            break;
          }
        }
      }
    } else {
      throw new WarpScriptException(getName() + " expects a LIST, MAP, STRING, GTS or BYTES.");
    }

    stack.push(indexes);
    return stack;
  }
}
