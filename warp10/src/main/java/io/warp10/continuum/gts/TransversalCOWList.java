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

import com.geoxp.GeoXPLib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 *
 * Copy on Write List implementation backed by a list of GTS
 * and corresponding to one field, either one of: latitudes, longitudes, elevations, values
 *
 * This is meant to be used by REDUCE and APPLY frameworks
 */
public class TransversalCOWList implements List {

  public static enum TYPE {
    LATITUDES, LONGITUDES, ELEVATIONS, VALUES
  }

  private final List<GeoTimeSerie> gtsList;
  private final int[] dataPointIndices; // these are the pointers that are updated during the reduce loop, one per gts
  private final int[] skippedGTSIndices; // these are the indices of gts from the gtsList that returns no value for the current aggregate
  private final TYPE type;

  public TransversalCOWList(List<GeoTimeSerie> gtsList, int[] indices, int[] skipped, TYPE type) {
    if (gtsList.size() != indices.length) {
      throw new RuntimeException("Size mismatch while constructing transversal aggregator");
    }

    this.gtsList = gtsList;
    this.dataPointIndices = indices;
    this.skippedGTSIndices = skipped;
    this.type = type;
    exposeNullValues = true;
  }

  /**
   * This field tracks if the null values must be exposed or not
   */
  private boolean exposeNullValues = true;

  public void setExposeNullValues(boolean exposeNullValues) {
    this.exposeNullValues = exposeNullValues;
  }

  public boolean isExposeNullValues() {
    return exposeNullValues;
  }

  /**
   * As long as readOnly is true, the List is backed by the GTS list (view).
   * As soon as user ask for a modification of the list, data are copied in an ArrayList, and readOnly turns false.
   */
  private boolean readOnly;
  private ArrayList mutableCopy = null;

  public boolean isReadOnly() {
    return readOnly;
  }

  private synchronized void initialDeepCopy() {
    if (readOnly) {
      int size = exposeNullValues ? gtsList.size() : gtsList.size() - skippedGTSIndices.length;
      mutableCopy = new ArrayList(size);

      // loop through each gts and extract its value at given index if it is not a skipped gts
      int skippedIdx = 0; // we assume skippedGTSIndices are sorted
      for (int i = 0; i < gtsList.size(); i++) {
        if (i == skippedGTSIndices[skippedIdx]) {
          if (exposeNullValues) {
            mutableCopy.add(null);
          }
          skippedIdx++; // this skipped gts has been seen so we increase the index

        } else {
          GeoTimeSerie gts = gtsList.get(i);

          switch (type) {
            case VALUES:
              switch (gts.type) {
                case DOUBLE:
                  mutableCopy.add(gts.doubleValues[dataPointIndices[i]]);
                  break;
                case LONG:
                  mutableCopy.add(gts.longValues[dataPointIndices[i]]);
                  break;
                case BOOLEAN:
                  mutableCopy.add(gts.booleanValues.get(dataPointIndices[i]));
                  break;
                case STRING:
                  mutableCopy.add(gts.stringValues[dataPointIndices[i]]);
                  break;
              }
              break;

            case ELEVATIONS:
              if (gts.hasElevations() && GeoTimeSerie.NO_ELEVATION != gts.elevations[dataPointIndices[i]]) {
                mutableCopy.add(gts.elevations[dataPointIndices[i]]);
              } else {
                mutableCopy.add(Double.NaN);
              }
              break;

            //todo(optimization): entangle latitudes and longitudes so that if writes trigger both list to be copied, then the conversion is done only once
            case LATITUDES:
              if (gts.hasLocations() && GeoTimeSerie.NO_LOCATION != gts.locations[dataPointIndices[i]]) {
                double lat = GeoXPLib.fromGeoXPPoint(gts.locations[dataPointIndices[i]])[0];
                mutableCopy.add(lat);
              } else {
                mutableCopy.add(Double.NaN);
              }
              break;

            case LONGITUDES:
              if (gts.hasLocations() && GeoTimeSerie.NO_LOCATION != gts.locations[dataPointIndices[i]]) {
                double lat = GeoXPLib.fromGeoXPPoint(gts.locations[dataPointIndices[i]])[1];
                mutableCopy.add(lat);
              } else {
                mutableCopy.add(Double.NaN);
              }
              break;
          }
        }
      }
      readOnly = false;
    }
  }

  //
  // Todo: following
  //

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean contains(Object o) {
    return false;
  }

  @Override
  public Iterator iterator() {
    return null;
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public boolean add(Object o) {
    return false;
  }

  @Override
  public boolean remove(Object o) {
    return false;
  }

  @Override
  public boolean addAll(Collection collection) {
    return false;
  }

  @Override
  public boolean addAll(int i, Collection collection) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public Object get(int i) {
    return null;
  }

  @Override
  public Object set(int i, Object o) {
    return null;
  }

  @Override
  public void add(int i, Object o) {

  }

  @Override
  public Object remove(int i) {
    return null;
  }

  @Override
  public int indexOf(Object o) {
    return 0;
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator listIterator() {
    return null;
  }

  @Override
  public ListIterator listIterator(int i) {
    return null;
  }

  @Override
  public List subList(int i, int i1) {
    return null;
  }

  @Override
  public boolean retainAll(Collection collection) {
    return false;
  }

  @Override
  public boolean removeAll(Collection collection) {
    return false;
  }

  @Override
  public boolean containsAll(Collection collection) {
    return false;
  }

  @Override
  public Object[] toArray(Object[] objects) {
    return new Object[0];
  }
}
