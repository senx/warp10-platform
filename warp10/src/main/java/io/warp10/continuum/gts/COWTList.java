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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

/**
 * Copy On Write Transversal List
 * It is transversal across a list of GTS
 * It is bounded to one field, either one of: locations, elevations, values
 *
 * For instance, it is used by REDUCE and APPLY frameworks
 */
public class COWTList implements List {

  public static enum TYPE {
    LOCATIONS, ELEVATIONS, VALUES
  }

  private final List<GeoTimeSerie> gtsList;
  private final int[] dataPointIndices; // these are the pointers that are updated during the reduce loop, one per gts
  private final List<Integer> skippedGTSIndices; // these are the indices of gts from the gtsList that returns no value for the current aggregate
  private final TYPE type;

  public COWTList(List<GeoTimeSerie> gtsList, int[] indices, List<Integer> skipped, TYPE type) {
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
   * As soon as user asks for a modification of the list, data are copied in an ArrayList, and readOnly turns false.
   */
  private boolean readOnly;
  private ArrayList mutableCopy = null;

  public boolean isReadOnly() {
    return readOnly;
  }

  private synchronized void initialDeepCopy() {
    if (readOnly) {
      mutableCopy = new ArrayList(size());

      // loop through each gts and extract its value at given index if it is not a skipped gts
      int skippedIdx = 0; // we assume skippedGTSIndices are sorted
      for (int i = 0; i < gtsList.size(); i++) {
        if (i == skippedGTSIndices.get(skippedIdx)) {
          if (exposeNullValues) {
            switch (type) {
              case VALUES:
                mutableCopy.add(null);
              case LOCATIONS:
                mutableCopy.add(GeoTimeSerie.NO_LOCATION);
              case ELEVATIONS:
                mutableCopy.add(GeoTimeSerie.NO_ELEVATION);
            }
          }
          skippedIdx++; // a skipped gts has been seen so we increase the index

        } else {
          GeoTimeSerie gts = gtsList.get(i + skippedIdx);
          int index = dataPointIndices[i + skippedIdx];

          switch (type) {
            case VALUES:
              switch (gts.type) {
                case DOUBLE:
                  mutableCopy.add(gts.doubleValues[index]);
                  break;
                case LONG:
                  mutableCopy.add(gts.longValues[index]);
                  break;
                case BOOLEAN:
                  mutableCopy.add(gts.booleanValues.get(index));
                  break;
                case STRING:
                  mutableCopy.add(gts.stringValues[index]);
                  break;
              }
              break;

            case LOCATIONS:
              if (gts.hasLocations()) {
                mutableCopy.add(gts.locations[index]);
              } else {
                mutableCopy.add(GeoTimeSerie.NO_LOCATION);
              }
              break;

            case ELEVATIONS:
              if (gts.hasElevations() && GeoTimeSerie.NO_ELEVATION != gts.elevations[index]) {
                mutableCopy.add(gts.elevations[index]);
              } else {
                mutableCopy.add(GeoTimeSerie.NO_ELEVATION);
              }
              break;
          }
        }
      }
      readOnly = false;
    }
  }

  @Override
  public int size() {
    if (readOnly) {
      return exposeNullValues ? gtsList.size() : gtsList.size() - skippedGTSIndices.size();
    } else {
      return mutableCopy.size();
    }
  }

  private void rangeCheck(int index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
    }
  }

  @Override
  public Object get(int i) {
    if (readOnly) {
      rangeCheck(i);

      int skipped = 0;
      for (int j = 0; j < skippedGTSIndices.size(); j++) {
        skipped++;
        if (i == skippedGTSIndices.get(j)) {
          if (exposeNullValues) {
            switch (type) {
              case VALUES:
                return null;
              case LOCATIONS:
                return GeoTimeSerie.NO_LOCATION;
              case ELEVATIONS:
                return GeoTimeSerie.NO_ELEVATION;
            }
          }
        }
      }

      GeoTimeSerie gts = gtsList.get(i + skipped);
      int index = dataPointIndices[i + skipped];

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

      return new COWTList(newList, newIndices, newSkipped, type);

    } else {
      return mutableCopy.subList(fromIndex, toIndex);
    }
  }

  // todo(refactoring): since most of the overrides below are the same than for COWList, they both could extend an AbstractCOWList class

  @Override
  public int indexOf(Object o) {
    if (readOnly) {
      for (int i = 0; i < size(); i++) {
        if (o.equals(get(i))) {
          return i;
        }
      }
      return -1;
    } else {
      return mutableCopy.indexOf(o);
    }
  }

  @Override
  public int lastIndexOf(Object o) {
    //todo
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return 0 == size();
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  @Override
  public Iterator iterator() {
    if (readOnly) {
      return new Iterator() {
        int cursor = 0;

        @Override
        public boolean hasNext() {
          return cursor < size();
        }

        @Override
        public Object next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          Object res = get(cursor);
          cursor++;
          return res;
        }

        public void remove() {}
      };

    } else {
      return mutableCopy.iterator();
    }
  }

  @Override
  public boolean add(Object o) {
    initialDeepCopy();
    return mutableCopy.add(o);
  }

  @Override
  public boolean remove(Object o) {
    initialDeepCopy();
    return mutableCopy.remove(o);
  }

  @Override
  public boolean addAll(Collection c) {
    initialDeepCopy();
    return mutableCopy.addAll(c);
  }

  @Override
  public boolean addAll(int i, Collection c) {
    initialDeepCopy();
    return mutableCopy.addAll(i, c);
  }

  @Override
  public void replaceAll(UnaryOperator operator) {
    initialDeepCopy();
    mutableCopy.replaceAll(operator);
  }

  @Override
  public void sort(Comparator c) {
    initialDeepCopy();
    mutableCopy.sort(c);
  }

  @Override
  public void clear() {
    mutableCopy = new ArrayList();
    readOnly = false;
  }

  @Override
  public Object set(int i, Object o) {
    initialDeepCopy();
    return mutableCopy.set(i, o);
  }

  @Override
  public void add(int i, Object o) {
    initialDeepCopy();
    mutableCopy.add(i, o);
  }

  @Override
  public Object remove(int i) {
    initialDeepCopy();
    return mutableCopy.remove(i);
  }

  @Override
  public ListIterator listIterator() {
    initialDeepCopy();
    return mutableCopy.listIterator();
  }

  @Override
  public ListIterator listIterator(int i) {

    initialDeepCopy();
    return mutableCopy.listIterator(i);
  }

  @Override
  public boolean retainAll(Collection c) {
    initialDeepCopy();
    return mutableCopy.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection c) {
    initialDeepCopy();
    return mutableCopy.removeAll(c);
  }

  @Override
  public boolean containsAll(Collection c) {
    for (Object e: c)
      if (!contains(e)) {
        return false;
      }
    return true;
  }

  @Override
  public Object[] toArray(Object[] a) {
    if (readOnly) {
      Object[] r = a;
      if (r.length < size()) {
        r = new Object[size()];
      }
      for (int i = 0; i < size(); i++) {
        r[i] = get(i);
      }
      return r;
    } else {
      return mutableCopy.toArray(a);
    }
  }

  @Override
  public Object[] toArray() {
    return toArray(new Object[size()]);
  }
}
