//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10.continuum.gts;

import java.util.ArrayList;
import java.util.List;

/**
 * Copy On Write Transversal List
 * It is transversal across a list of GTS
 * It is bounded to one field, either one of: locations, elevations, values
 *
 * For instance, it is used by REDUCE and APPLY frameworks
 */
public class COWTList extends AbstractCOWList {

  public static enum TYPE {
    LOCATIONS, ELEVATIONS, VALUES
  }

  private final List<GeoTimeSerie> gtsList;
  private final int[] dataPointIndices; // these are the pointers that are updated during the reduce loop, one per gts
  private final List<Integer> skippedGTSIndices; // these are the indices of gts from the gtsList that returns no value for the current aggregate
  private final TYPE type;
  private final long referenceTick;

  public COWTList(List<GeoTimeSerie> gtsList, int[] indices, List<Integer> skipped, long referenceTick, TYPE type) {
    if (gtsList.size() != indices.length) {
      throw new RuntimeException("Size mismatch while constructing transversal aggregator");
    }

    this.gtsList = gtsList;
    this.dataPointIndices = indices;
    this.skippedGTSIndices = skipped;
    this.referenceTick = referenceTick;
    this.type = type;
    exposeNullValues = true;
  }

  /**
   * This field tracks if the null values must be exposed or not
   */
  private boolean exposeNullValues = true;

  // sub collections are used if null values are not exposed
  private List<GeoTimeSerie> subGTSList = null;
  private int[] subDataPointIndices = null;

  public void setExposeNullValues(boolean exposeNullValues) {
    if (exposeNullValues == this.exposeNullValues) {
      return;
    }

    this.exposeNullValues = exposeNullValues;
    if (!exposeNullValues && null == subGTSList) {
      subGTSList = new ArrayList<GeoTimeSerie>(size());
      subDataPointIndices = new int[size()];

      int count = 0;
      for (int i = 0; i < gtsList.size(); i++) {
        if (!isNullAt(i)) {
          subGTSList.add(gtsList.get(i));
          subDataPointIndices[count++] = dataPointIndices[i];
        }
        if (count == size()) {
          break;
        }
      }
    }
  }

  public boolean isExposeNullValues() {
    return exposeNullValues;
  }

  @Override
  public int size() {
    if (readOnly) {
      return exposeNullValues ? gtsList.size() : gtsList.size() - skippedGTSIndices.size();
    } else {
      return mutableCopy.size();
    }
  }

  private boolean isNullAt(int i) {
    return dataPointIndices[i] >= gtsList.get(i).values || referenceTick != gtsList.get(i).ticks[dataPointIndices[i]];
  }

  @Override
  public Object get(int i) {
    if (readOnly) {
      rangeCheck(i);

      if (exposeNullValues && isNullAt(i)) {
        switch (type) {
          case VALUES:
            return null;
          case LOCATIONS:
            return GeoTimeSerie.NO_LOCATION;
          case ELEVATIONS:
            return GeoTimeSerie.NO_ELEVATION;
        }
      }
      
      GeoTimeSerie gts = exposeNullValues ? gtsList.get(i) : subGTSList.get(i);
      int index = exposeNullValues ? dataPointIndices[i] : subDataPointIndices[i];

      switch (type) {
        case VALUES:
          switch (gts.type) {
            case DOUBLE:
              return gts.doubleValues[index];
            case LONG:
              return gts.longValues[index];
            case BOOLEAN:
              return gts.booleanValues.get(index);
            case STRING:
              return gts.stringValues[index];
          }
          break;

        case LOCATIONS:
          if (gts.hasLocations()) {
            return gts.locations[index];
          } else {
            return GeoTimeSerie.NO_LOCATION;
          }

        case ELEVATIONS:
          if (gts.hasElevations() && GeoTimeSerie.NO_ELEVATION != gts.elevations[index]) {
            return gts.elevations[index];
          } else {
            return GeoTimeSerie.NO_ELEVATION;
          }
      }

    } else {
      return mutableCopy.get(i);
    }

    // this line should not be reached
    return null;
  }

  @Override
  public List subList(int fromIndex, int toIndex) {
    if (readOnly) {
      rangeCheck(fromIndex);
      int newSize = toIndex - fromIndex;
      if (newSize < 0 || newSize + toIndex > size()) {
        throw new IndexOutOfBoundsException("Start index(" + fromIndex + ") + length(" + newSize + ") greater than original array size(" + size() + "), cannot create sublist.");
      }

      List newList = new ArrayList(gtsList.subList(fromIndex,toIndex));
      int[] newIndices = new int[newSize];
      for (int i = fromIndex; i < toIndex; i++) {
        newIndices[i - fromIndex] = dataPointIndices[i];
      }

      int newSkippedSize = 0;
      int firstSkippedIdx = -1;
      int lastSkippedIdx = -1;
      for (int i = 0; i < skippedGTSIndices.size(); i++) {
        if (skippedGTSIndices.get(i) >= fromIndex && skippedGTSIndices.get(i) < toIndex) {
          newSkippedSize++;
          lastSkippedIdx = i;
          if (-1 == firstSkippedIdx) {
            firstSkippedIdx = i;
          }
        }
      }
      List<Integer> newSkipped = new ArrayList<Integer>(newSkippedSize);
      if (newSkippedSize > 0) {
        for (int i = firstSkippedIdx; i < lastSkippedIdx + 1; i++) {
          newSkipped.set(i - firstSkippedIdx, i - fromIndex);
        }
      }

      return new COWTList(newList, newIndices, newSkipped, referenceTick, type);

    } else {
      return mutableCopy.subList(fromIndex, toIndex);
    }
  }
}
