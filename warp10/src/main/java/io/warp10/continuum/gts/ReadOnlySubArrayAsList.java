//
//   Copyright 2022  SenX S.A.S.
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

import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

/**
 * Read Only List object that can wrap arrays or bitset into sublists
 */
public class ReadOnlySubArrayAsList implements List {

  private final int size;
  private final int startIdx; // index of first element of sublist, inclusive
  private final int virtualSize; // length of the requested sublist

  private long[] dataLong = null;
  private double[] dataDouble = null;
  private String[] dataString = null;
  private BitSet dataBoolean = null;

  public static enum TYPE {
    LONG, DOUBLE, BOOLEAN, STRING
  }

  public final TYPE dataType;

  /**
   * Wrap an array of long (primitive) into a subList
   *
   * @param elementData array of long
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public ReadOnlySubArrayAsList(long[] elementData, int startIdx, int length) {
    this.dataLong = elementData;
    this.size = elementData.length;
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.LONG;
    this.startIdx = startIdx;
    this.virtualSize = length;
  }

  /**
   * Wrap an array of double (primitive) into a subList
   *
   * @param elementData array of double
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public ReadOnlySubArrayAsList(double[] elementData, int startIdx, int length) {
    this.dataDouble = elementData;
    this.size = elementData.length;
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.DOUBLE;
    this.startIdx = startIdx;
    this.virtualSize = length;
  }

  /**
   * Wrap a BitSet into a subList
   *
   * @param elementData BitSet
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed BitSet size
   */
  public ReadOnlySubArrayAsList(BitSet elementData, int startIdx, int length) {
    this.dataBoolean = elementData;
    this.size = elementData.size();
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.BOOLEAN;
    this.startIdx = startIdx;
    this.virtualSize = length;
  }

  /**
   * Wrap an array of String into a subList
   *
   * @param elementData array of String
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public ReadOnlySubArrayAsList(String[] elementData, int startIdx, int length) {
    this.dataString = elementData;
    this.size = elementData.length;
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.STRING;
    this.startIdx = startIdx;
    this.virtualSize = length;
  }

  private void initialRangeCheck(int startIndex, int length) {
    if (startIndex >= size || startIndex < 0) {
      throw new IndexOutOfBoundsException(outOfBoundsMsg(startIndex));
    }
    if (length < 0 || (startIndex + length) > size) {
      throw new IndexOutOfBoundsException("Start index(" + startIndex + ") + length(" + length + ") is greater than original array size(" + size + "), cannot create sublist");
    }
  }

  private void rangeCheck(int index) {
    if (index < 0 || index >= virtualSize) {
      throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
    }
  }

  private String outOfBoundsMsg(int index) {
    return "Index: " + index + ", Size: " + size;
  }

  @Override
  public int indexOf(Object o) {
    if (o == null) {
      return -1; // no null object in a primitive array
    }
    switch (dataType) {
      case LONG:
        for (int i = 0; i < virtualSize; i++) {
          if (o.equals(dataLong[i + startIdx])) {
            return i;
          }
        }
        break;
      case DOUBLE:
        for (int i = 0; i < virtualSize; i++) {
          if (o.equals(dataDouble[i + startIdx])) {
            return i;
          }
        }
        break;
      case STRING:
        for (int i = 0; i < virtualSize; i++) {
          if (o.equals(dataString[i + startIdx])) {
            return i;
          }
        }
        break;
      case BOOLEAN:
        for (int i = 0; i < virtualSize; i++) {
          if (o.equals(dataBoolean.get(i + startIdx))) {
            return i;
          }
        }
        break;
    }
    return -1;
  }

  @Override
  public int size() {
    return virtualSize;
  }

  @Override
  public boolean isEmpty() {
    return 0 == virtualSize;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }


  private class Itr implements Iterator<Object> {
    int cursor;       // index of next element to return

    public boolean hasNext() {
      return cursor != virtualSize;
    }

    public Object next() {
      int i = cursor;
      if (i >= virtualSize) {
        throw new NoSuchElementException();
      }
      Object res = null;
      switch (dataType) {
        case LONG:
          res = dataLong[startIdx + i];
          break;
        case DOUBLE:
          res = dataDouble[startIdx + i];
          break;
        case STRING:
          res = dataString[startIdx + i];
          break;
        case BOOLEAN:
          res = dataBoolean.get(startIdx + i);
          break;
      }
      cursor = i + 1;
      return res;
    }

    public void remove() {
      // not applicable
    }

  }

  @Override
  public Iterator iterator() {
    return new Itr();
  }

  /**
   * Modification of the underlying array or bitset is forbidden. add will raise a runtime exception.
   */
  @Override
  public boolean add(Object o) {
    throw new RuntimeException("This is a read only list, cannot add element.");
  }

  /**
   * Modification of the underlying array or bitset is forbidden. remove will raise a runtime exception.
   */
  @Override
  public boolean remove(Object o) {
    throw new RuntimeException("This is a read only list, cannot remove element.");
  }

  /**
   * Modification of the underlying array or bitset is forbidden. addAll will raise a runtime exception.
   */
  @Override
  public boolean addAll(Collection c) {
    throw new RuntimeException("This is a read only list, cannot add element.");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. addAll will raise a runtime exception.
   */
  @Override
  public boolean addAll(int index, Collection c) {
    throw new RuntimeException("This is a read only list, cannot add element.");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. replaceAll will raise a runtime exception.
   */
  @Override
  public void replaceAll(UnaryOperator operator) {
    throw new RuntimeException("This is a read only list, cannot overwrite element.");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. sort will raise a runtime exception.
   */
  @Override
  public void sort(Comparator c) {
    throw new RuntimeException("This is a read only list, cannot sort elements.");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. clear will raise a runtime exception.
   */
  @Override
  public void clear() {
    throw new RuntimeException("This is a read only list, cannot remove elements");
  }

  /**
   * get will return the element at index + startidx of the underlying array or bitset.
   *
   * @param index cannot be less than 0 or greater than list length
   * @return
   */
  @Override
  public Object get(int index) {
    rangeCheck(index);
    switch (dataType) {
      case LONG:
        return (Long) dataLong[index + startIdx];
      case DOUBLE:
        return (Double) dataDouble[index + startIdx];
      case STRING:
        return dataString[index + startIdx];
      case BOOLEAN:
        return dataBoolean.get(index + startIdx);
    }
    return null; // impossible
  }


  /**
   * Modification of the underlying array or bitset is forbidden. set will raise a runtime exception.
   */
  @Override
  public Object set(int index, Object element) {
    throw new RuntimeException("This is a read only list, cannot overwrite element.");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. add will raise a runtime exception.
   */
  @Override
  public void add(int index, Object element) {
    throw new RuntimeException("This is a read only list, cannot overwrite element.");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. remove will raise a runtime exception.
   */
  @Override
  public Object remove(int index) {
    throw new RuntimeException("This is a read only list, cannot remove element.");
  }

  @Override
  public int lastIndexOf(Object o) {
    if (o == null) {
      return -1; // no null object in a primitive array
    }
    switch (dataType) {
      case LONG:
        for (int i = virtualSize - 1; i >= 0; i--) {
          if (o.equals(dataLong[i + startIdx])) {
            return i;
          }
        }
        break;
      case DOUBLE:
        for (int i = virtualSize - 1; i >= 0; i--) {
          if (o.equals(dataDouble[i + startIdx])) {
            return i;
          }
        }
        break;
      case STRING:
        for (int i = virtualSize - 1; i >= 0; i--) {
          if (o.equals(dataString[i + startIdx])) {
            return i;
          }
        }
        break;
      case BOOLEAN:
        for (int i = virtualSize - 1; i >= 0; i--) {
          if (o.equals(dataBoolean.get(i + startIdx))) {
            return i;
          }
        }
        break;
    }
    return -1;
  }


  @Override
  public ListIterator listIterator() {
    throw new RuntimeException("This is a read only list, cannot make a listIterator.");
  }

  @Override
  public ListIterator listIterator(int index) {
    return null;
  }

  /**
   * create a new Read Only subList from existing one, backed by the original array.
   * Returns a view of the portion of this list between the specified
   * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.  (If
   * {@code fromIndex} and {@code toIndex} are equal, the returned list is
   * empty.)
   */
  @Override
  public List subList(int fromIndex, int toIndex) {
    rangeCheck(fromIndex);
    int subsize = toIndex - fromIndex;
    if (subsize < 0 || subsize + toIndex > virtualSize) {
      throw new IndexOutOfBoundsException("Start index(" + fromIndex + ") + length(" + subsize + ") greater than original array size(" + virtualSize + "), cannot create sublist.");
    }
    List r = null;
    switch (dataType) {
      case LONG:
        r = new ReadOnlySubArrayAsList(dataLong, fromIndex + startIdx, subsize);
        break;
      case DOUBLE:
        r = new ReadOnlySubArrayAsList(dataDouble, fromIndex + startIdx, subsize);
        break;
      case STRING:
        r = new ReadOnlySubArrayAsList(dataString, fromIndex + startIdx, subsize);
        break;
      case BOOLEAN:
        r = new ReadOnlySubArrayAsList(dataBoolean, fromIndex + startIdx, subsize);
        break;
    }
    return r;
  }

  @Override
  public boolean retainAll(Collection c) {
    throw new RuntimeException("This is a read only list, cannot alter list content.");
  }

  @Override
  public boolean removeAll(Collection c) {
    throw new RuntimeException("This is a read only list, cannot remove elements.");
  }

  @Override
  public boolean containsAll(Collection c) {
    throw new RuntimeException("This is a read only list, containsAll is not supported.");
  }

  /**
   * creates a new long/double/String/boolean array from the current subList (deep copy).
   * if the array provided as {@code a} is not big enough, toArray allocates and returns a new array
   */
  @Override
  public Object[] toArray(Object[] a) {
    Object[] r = a;
    if (r.length < virtualSize) {
      r = new Object[virtualSize];
    }
    switch (dataType) {
      case LONG:
        for (int i = 0; i < virtualSize; i++) {
          r[i] = (Long) dataLong[i + startIdx];
        }
        break;
      case DOUBLE:
        for (int i = 0; i < virtualSize; i++) {
          r[i] = (Double) dataDouble[i + startIdx];
        }
        break;
      case STRING:
        System.arraycopy(dataString,0,r,0,virtualSize);
        break;
      case BOOLEAN:
        for (int i = 0; i < virtualSize; i++) {
          r[i] = (Boolean) dataBoolean.get(i + startIdx);
        }
        break;
    }
    return r;
  }

  /**
   * creates a new long/double/String/boolean array from the current subList (deep copy).
   */
  @Override
  public Object[] toArray() {
    return toArray(new Object[virtualSize]);
  }
}
