//
//   Copyright 2018-2023  SenX S.A.S.
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

import gnu.trove.list.array.TLongArrayList;
import io.warp10.ThriftUtils;
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

    TDeserializer deserializer = ThriftUtils.getTDeserializer(new TCompactProtocol.Factory());

    GTSWrapper wrapper = new GTSWrapper();

    try {
      deserializer.deserialize(wrapper, serialized);
    } catch (TException te) {
      throw new WarpScriptException(te);
    }

    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);

    int count = (int) wrapper.getCount();
    long[] cells = new long[count];
    GeoXPShape shape;
    boolean erroneousCount = false;

    int idx = 0;

    // In most cases, the count provided by the wrapper will be the correct one, so the code is optimized for
    // that case. If this is not the case, the following will fall back to a TLongArrayList to have a growing
    // array of longs. Even if we want the array of cells not to contain any duplicate an be sorted, we avoid
    // using a TreeSet as performance tests proved this solution to be much slower.

    while (decoder.next()) {
      // We are only interested in the timestamp which is the cell
      long cell = decoder.getTimestamp();
      // Only add cells with valid resolution (1-15)
      if (0L != (cell & 0xf000000000000000L)) {
        if (idx >= count) {
          erroneousCount = true;
          break;
        }
        cells[idx++] = cell;
      }
    }

    if(erroneousCount) {
      // The count provided by the wrapper is wrong, so we use a TLongArrayList to have a growing array of longs.
      TLongArrayList cellsList = new TLongArrayList(cells.length + 1);

      // Add all already decoded longs and the current one, which is already tested to be valid.
      cellsList.add(cells);
      cellsList.add(decoder.getTimestamp());

      // Decode the rest of the cells.
      while (decoder.next()) {
        // We are only interested in the timestamp which is the cell
        long cell = decoder.getTimestamp();
        // Only add cells with valid resolution (1-15)
        if (0L != (cell & 0xf000000000000000L)) {
          cellsList.add(cell);
        }
      }

      // Make sure the cells are sorted
      cellsList.sort();

      // Make sure there are no duplicates.
      long lastCell = 0;
      int noOfDups = 0;

      for (int i = 0; i < cellsList.size(); i++) {
        long cell = cellsList.get(i);

        if (i != 0) {
          if (lastCell == cell) {
            noOfDups++;
          } else if (0 != noOfDups) {
            cellsList.set(i - noOfDups, cell);
          }
        }

        lastCell = cell;
      }

      // Build the shape from the sorted and deduplicated cells.
      shape = GeoXPLib.fromCells(cellsList.toArray(0, cellsList.size() - noOfDups), false);
    } else {
      // Make sure the cells are sorted
      Arrays.sort(cells, 0, idx);

      // Make sure there are no duplicates.
      long lastCell = 0;
      int noOfDups = 0;

      for (int i = 0; i < idx; i++) {
        long cell = cells[i];

        if (i != 0) {
          if (lastCell == cell) {
            noOfDups++;
          } else if (0 != noOfDups) {
            cells[i - noOfDups] = cell;
          }
        }

        lastCell = cell;
      }

      // Update the actual count of cells in the array.
      idx -= noOfDups;

      // Adjust the size if there were some invalid or duplicate cells (timestamps).
      // This can happen when calling GEOUNPACK from a WRAPped GTS or ENCODER
      if (idx != cells.length) {
        cells = Arrays.copyOf(cells, idx);
      }

      // Build the shape from the sorted and deduplicated cells.
      shape = GeoXPLib.fromCells(cells, false);
    }

    stack.push(shape);

    return stack;
  }
}
