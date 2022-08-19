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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Read Only List object that can wrap arrays or bitset into sublists
 */
public class ReadOnlySubArrayAsList implements List {

  private final int size;
  private final int startidx; // index of first element of sublist, inclusive
  private final int virtualsize; // length of the requested sublist

  private long[] elementDataLong = null;
  private double[] elementDataDouble = null;
  private String[] elementDataString = null;
  private BitSet elementDataBoolean = null;

  public static enum TYPE {
    LONG, DOUBLE, BOOLEAN, STRING
  }

  public final TYPE elementDataType;

  /**
   * Wrap an array of long (primitive) into a subList
   *
   * @param elementData array of long
   * @param startidx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public ReadOnlySubArrayAsList(long[] elementData, int startidx, int length) {
    this.elementDataLong = elementData;
    this.size = elementData.length;
    initialRangeCheck(startidx, length);
    this.elementDataType = TYPE.LONG;
    this.startidx = startidx;
    this.virtualsize = length;
  }

  /**
   * Wrap an array of double (primitive) into a subList
   *
   * @param elementData array of double
   * @param startidx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public ReadOnlySubArrayAsList(double[] elementData, int startidx, int length) {
    this.elementDataDouble = elementData;
    this.size = elementData.length;
    initialRangeCheck(startidx, length);
    this.elementDataType = TYPE.DOUBLE;
    this.startidx = startidx;
    this.virtualsize = length;
  }

  /**
   * Wrap a BitSet into a subList
   *
   * @param elementData BitSet
   * @param startidx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed BitSet size
   */
  public ReadOnlySubArrayAsList(BitSet elementData, int startidx, int length) {
    this.elementDataBoolean = elementData;
    this.size = elementData.size();
    initialRangeCheck(startidx, length);
    this.elementDataType = TYPE.BOOLEAN;
    this.startidx = startidx;
    this.virtualsize = length;
  }

  /**
   * Wrap an array of String into a subList
   *
   * @param elementData array of String
   * @param startidx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public ReadOnlySubArrayAsList(String[] elementData, int startidx, int length) {
    this.elementDataString = elementData;
    this.size = elementData.length;
    initialRangeCheck(startidx, length);
    this.elementDataType = TYPE.STRING;
    this.startidx = startidx;
    this.virtualsize = length;
  }

  private void initialRangeCheck(int startindex, int length) {
    if (startindex >= size || startindex < 0) {
      throw new IndexOutOfBoundsException(outOfBoundsMsg(startindex));
    }
    if (length < 0 || (startindex + length) > size) {
      throw new IndexOutOfBoundsException("start index(" + startindex + ") + length(" + length + ") greater than original array size(" + size + "), cannot create sublist");
    }
  }

  private void rangeCheck(int index) {
    if (index < 0 || index >= virtualsize) {
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
    switch (elementDataType) {
      case LONG:
        for (int i = 0; i < virtualsize; i++) {
          if (o.equals(elementDataLong[i + startidx])) {
            return i;
          }
        }
        break;
      case DOUBLE:
        for (int i = 0; i < virtualsize; i++) {
          if (o.equals(elementDataDouble[i + startidx])) {
            return i;
          }
        }
        break;
      case STRING:
        for (int i = 0; i < virtualsize; i++) {
          if (o.equals(elementDataString[i + startidx])) {
            return i;
          }
        }
        break;
      case BOOLEAN:
        for (int i = 0; i < virtualsize; i++) {
          if (o.equals(elementDataBoolean.get(i + startidx))) {
            return i;
          }
        }
        break;
    }
    return -1;
  }

  @Override
  public int size() {
    return virtualsize;
  }

  @Override
  public boolean isEmpty() {
    return 0 == virtualsize;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }


  private class Itr implements Iterator<Object> {
    int cursor;       // index of next element to return
    int lastRet = -1; // index of last element returned; -1 if no such

    Itr() {
    }

    public boolean hasNext() {
      return cursor != virtualsize;
    }

    public Object next() {
      int i = cursor;
      if (i >= virtualsize) {
        throw new NoSuchElementException();
      }
      Object res = null;
      switch (elementDataType) {
        case LONG:
          res = elementDataLong[startidx + i];
          break;
        case DOUBLE:
          res = elementDataDouble[startidx + i];
          break;
        case STRING:
          res = elementDataString[startidx + i];
          break;
        case BOOLEAN:
          res = elementDataBoolean.get(startidx + i);
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
    throw new RuntimeException("this is a read only list, cannot add element");
  }

  /**
   * Modification of the underlying array or bitset is forbidden. remove will raise a runtime exception.
   */
  @Override
  public boolean remove(Object o) {
    throw new RuntimeException("this is a read only list, cannot remove element");
  }

  /**
   * Modification of the underlying array or bitset is forbidden. addAll will raise a runtime exception.
   */
  @Override
  public boolean addAll(Collection c) {
    throw new RuntimeException("this is a read only list, cannot add element");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. addAll will raise a runtime exception.
   */
  @Override
  public boolean addAll(int index, Collection c) {
    throw new RuntimeException("this is a read only list, cannot add element");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. replaceAll will raise a runtime exception.
   */
  @Override
  public void replaceAll(UnaryOperator operator) {
    throw new RuntimeException("this is a read only list, cannot overwrite element");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. sort will raise a runtime exception.
   */
  @Override
  public void sort(Comparator c) {
    throw new RuntimeException("this is a read only list, cannot sort elements");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. clear will raise a runtime exception.
   */
  @Override
  public void clear() {
    throw new RuntimeException("this is a read only list, cannot remove elements");
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
    switch (elementDataType) {
      case LONG:
        return (Long) elementDataLong[index + startidx];
      case DOUBLE:
        return (Double) elementDataDouble[index + startidx];
      case STRING:
        return elementDataString[index + startidx];
      case BOOLEAN:
        return elementDataBoolean.get(index + startidx);
    }
    return null; // impossible
  }


  /**
   * Modification of the underlying array or bitset is forbidden. set will raise a runtime exception.
   */
  @Override
  public Object set(int index, Object element) {
    throw new RuntimeException("this is a read only list, cannot overwrite element");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. add will raise a runtime exception.
   */
  @Override
  public void add(int index, Object element) {
    throw new RuntimeException("this is a read only list, cannot overwrite element");
  }


  /**
   * Modification of the underlying array or bitset is forbidden. remove will raise a runtime exception.
   */
  @Override
  public Object remove(int index) {
    throw new RuntimeException("this is a read only list, cannot remove element");
  }

  @Override
  public int lastIndexOf(Object o) {
    if (o == null) {
      return -1; // no null object in a primitive array
    }
    switch (elementDataType) {
      case LONG:
        for (int i = virtualsize - 1; i >= 0; i--) {
          if (o.equals(elementDataLong[i + startidx])) {
            return i;
          }
        }
        break;
      case DOUBLE:
        for (int i = virtualsize - 1; i >= 0; i--) {
          if (o.equals(elementDataDouble[i + startidx])) {
            return i;
          }
        }
        break;
      case STRING:
        for (int i = virtualsize - 1; i >= 0; i--) {
          if (o.equals(elementDataString[i + startidx])) {
            return i;
          }
        }
        break;
      case BOOLEAN:
        for (int i = virtualsize - 1; i >= 0; i--) {
          if (o.equals(elementDataBoolean.get(i + startidx))) {
            return i;
          }
        }
        break;
    }
    return -1;
  }


  @Override
  public ListIterator listIterator() {
    throw new RuntimeException("this is a read only list, cannot make a listIterator");
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
    if (subsize < 0 || subsize + toIndex > virtualsize) {
      throw new IndexOutOfBoundsException("start index(" + fromIndex + ") + length(" + subsize + ") greater than original array size(" + virtualsize + "), cannot create sublist");
    }
    List r = null;
    switch (elementDataType) {
      case LONG:
        r = new ReadOnlySubArrayAsList(elementDataLong, fromIndex + startidx, subsize);
        break;
      case DOUBLE:
        r = new ReadOnlySubArrayAsList(elementDataDouble, fromIndex + startidx, subsize);
        break;
      case STRING:
        r = new ReadOnlySubArrayAsList(elementDataString, fromIndex + startidx, subsize);
        break;
      case BOOLEAN:
        r = new ReadOnlySubArrayAsList(elementDataBoolean, fromIndex + startidx, subsize);
        break;
    }
    return r;
  }

  @Override
  public Spliterator spliterator() {
    return List.super.spliterator();
  }

  @Override
  public boolean retainAll(Collection c) {
    throw new RuntimeException("this is a read only list, cannot alter list content");
  }

  @Override
  public boolean removeAll(Collection c) {
    throw new RuntimeException("this is a read only list, cannot remove elements");
  }

  @Override
  public boolean containsAll(Collection c) {
    throw new RuntimeException("this is a read only list, containsAll is not supported");
  }

  /**
   * creates a new long/double/String/boolean array from the current subList (deep copy).
   * if the array provided as {@code a} is not big enough, toArray allocates and returns a new array
   */
  @Override
  public Object[] toArray(Object[] a) {
    Object[] r = a;
    if (r.length < virtualsize) {
      r = new Object[virtualsize];
    }
    switch (elementDataType) {
      case LONG:
        for (int i = 0; i < virtualsize; i++) {
          r[i] = (Long) elementDataLong[i + startidx];
        }
        break;
      case DOUBLE:
        for (int i = 0; i < virtualsize; i++) {
          r[i] = (Double) elementDataDouble[i + startidx];
        }
        break;
      case STRING:
        for (int i = 0; i < virtualsize; i++) {
          r[i] = elementDataString[i + startidx];
        }
        break;
      case BOOLEAN:
        for (int i = 0; i < virtualsize; i++) {
          r[i] = (Boolean) elementDataBoolean.get(i + startidx);
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
    return toArray(new Object[virtualsize]);
  }
}
