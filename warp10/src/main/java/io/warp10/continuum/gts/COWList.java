//
//   Copyright 2022-2023  SenX S.A.S.
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
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

/**
 * Copy On Write List implementation backed by an existing array or bitset.
 * Data is duplicated when the list is modified.
 * The original array or bitset is never modified.
 */
public class COWList implements List {

  private final int size;
  private int startIdx; // index of first element of sublist, inclusive
  private final int virtualSize; // length of the requested sublist

  private long[] dataLong = null;
  private double[] dataDouble = null;
  private String[] dataString = null;
  private BitSet dataBoolean = null;

  /**
   * As long as readOnly is true, the List is backed by the original array (view). You can trust dataType.
   * As soon as user ask for a modification of the list, data are copied in an ArrayList, and readOnly turns false.
   * When backed by the ArrayList, dataType can be heterogeneous
   */
  private boolean readOnly;
  private ArrayList mutableCopy = null;

  public static enum TYPE {
    LONG, DOUBLE, BOOLEAN, STRING
  }

  private final TYPE dataType;

  /**
   * getDataType is only valid when the list has not been altered.
   * 
   */
  public TYPE getDataType() {
    if (readOnly) {
      return dataType;
    } else {
      throw new RuntimeException("The list has been modified and may contains heterogeneous elements");
    }
  }
  
  /**
   * Wrap an array of long (primitive) into a Copy On Write SubList
   *
   * @param elementData array of long
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public COWList(long[] elementData, int startIdx, int length) {
    this.dataLong = elementData;
    this.size = elementData.length;
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.LONG;
    this.startIdx = startIdx;
    this.virtualSize = length;
    this.readOnly = true;
  }

  /**
   * Wrap an array of double (primitive) into a Copy On Write SubList
   *
   * @param elementData array of double
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public COWList(double[] elementData, int startIdx, int length) {
    this.dataDouble = elementData;
    this.size = elementData.length;
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.DOUBLE;
    this.startIdx = startIdx;
    this.virtualSize = length;
    this.readOnly = true;
  }

  /**
   * Wrap a BitSet into a Copy On Write SubList
   *
   * @param elementData BitSet
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed BitSet size
   */
  public COWList(BitSet elementData, int startIdx, int length) {
    this.dataBoolean = elementData;
    this.size = elementData.size();
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.BOOLEAN;
    this.startIdx = startIdx;
    this.virtualSize = length;
    this.readOnly = true;
  }

  /**
   * Wrap an array of String into a Copy On Write SubList
   *
   * @param elementData array of String
   * @param startIdx    index of the first element
   * @param length      length of the subList. startidx+length cannot exceed elementData.length
   */
  public COWList(String[] elementData, int startIdx, int length) {
    this.dataString = elementData;
    this.size = elementData.length;
    initialRangeCheck(startIdx, length);
    this.dataType = TYPE.STRING;
    this.startIdx = startIdx;
    this.virtualSize = length;
    this.readOnly = true;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  private synchronized void initialDeepCopy() {
      if (readOnly) {
        mutableCopy = new ArrayList(virtualSize);
        switch (dataType) {
          case LONG:
            for (int i = 0; i < virtualSize; i++) {
              mutableCopy.add(dataLong[i + startIdx]);
            }
            break;
          case DOUBLE:
            for (int i = 0; i < virtualSize; i++) {
              mutableCopy.add(dataDouble[i + startIdx]);
            }
            break;
          case STRING:
            for (int i = 0; i < virtualSize; i++) {
              mutableCopy.add(dataString[i + startIdx]);
            }
            break;
          case BOOLEAN:
            for (int i = 0; i < virtualSize; i++) {
              mutableCopy.add(dataBoolean.get(i + startIdx));
            }
            break;
        }
        readOnly = false;
      }
    
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
    if (readOnly) {
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
    } else {
      return mutableCopy.indexOf(o);
    }
  }

  @Override
  public int size() {
    if (readOnly) {
      return virtualSize;
    } else {
      return mutableCopy.size();
    }
  }

  @Override
  public boolean isEmpty() {
    if (readOnly) {
      return 0 == virtualSize;
    } else {
      return mutableCopy.isEmpty();
    }
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }
  
  @Override
  public Iterator iterator() {
    if (readOnly) {
      return new Iterator() {

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
      };
    } else {
      return mutableCopy.iterator();
    }
  }

  /**
   * Modification of the underlying array or bitset is forbidden. 
   * add will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public boolean add(Object o) {
    initialDeepCopy();
    return mutableCopy.add(o);
  }

  /**
   * Modification of the underlying array or bitset is forbidden. 
   * remove will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public boolean remove(Object o) {
    initialDeepCopy();
    return mutableCopy.remove(o);
  }

  /**
   * Modification of the underlying array or bitset is forbidden. 
   * addAll will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public boolean addAll(Collection c) {
    initialDeepCopy();
    return mutableCopy.addAll(c);
  }


  /**
   * Modification of the underlying array or bitset is forbidden. 
   * addAll will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public boolean addAll(int index, Collection c) {
    initialDeepCopy();
    return mutableCopy.addAll(index, c);
  }


  /**
   * Modification of the underlying array or bitset is forbidden. 
   * replaceAll will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public void replaceAll(UnaryOperator operator) {
    initialDeepCopy();
    mutableCopy.replaceAll(operator);
  }


  /**
   * Modification of the underlying array or bitset is forbidden. 
   * sort will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public void sort(Comparator c) {
    initialDeepCopy();
    mutableCopy.sort(c);
  }


  /**
   * Modification of the underlying array or bitset is forbidden. 
   * clear will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public synchronized void clear() {
      mutableCopy = new ArrayList();
      readOnly = false;
  }

  /**
   * If not altered, get will return the element at index + startidx of the underlying array or bitset.
   * If altered, get will return the element of the underlying ArrayList.
   *
   * @param index cannot be less than 0 or greater than list length
   * @return element at index + startidx
   */
  @Override
  public Object get(int index) {
    if (!readOnly) {
      return this.mutableCopy.get(index);
    }
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
   * Modification of the underlying array or bitset is forbidden. 
   * set will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public Object set(int index, Object element) {
    initialDeepCopy();
    return mutableCopy.set(index, element);
  }


  /**
   * Modification of the underlying array or bitset is forbidden. 
   * add will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public void add(int index, Object element) {
    initialDeepCopy();
    mutableCopy.add(index, element);
  }


  /**
   * Modification of the underlying array or bitset is forbidden. 
   * remove will allocate a new ArrayList, then alter this ArrayList.
   */
  @Override
  public Object remove(int index) {
    initialDeepCopy();
    return mutableCopy.remove(index);
  }

  @Override
  public int lastIndexOf(Object o) {
    if (readOnly) {
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
    } else {
      return mutableCopy.lastIndexOf(o);
    }
  }


  @Override
  public ListIterator listIterator() {
    initialDeepCopy();
    return mutableCopy.listIterator();
  }

  @Override
  public ListIterator listIterator(int index) {
    initialDeepCopy();
    return mutableCopy.listIterator(index);
  }

  /**
   * When non altered, returns a new copy on write subList from existing one, backed by the original array.
   * When altered, returns a new ArrayList, backed by the same array
   * The portion of the list is specified by
   * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive.  (If
   * {@code fromIndex} and {@code toIndex} are equal, the returned list is
   * empty.)
   */
  @Override
  public List subList(int fromIndex, int toIndex) {
    if (readOnly) {
      rangeCheck(fromIndex);
      int subsize = toIndex - fromIndex;
      if (subsize < 0 || subsize + toIndex > virtualSize) {
        throw new IndexOutOfBoundsException("Start index(" + fromIndex + ") + length(" + subsize + ") greater than original array size(" + virtualSize + "), cannot create sublist.");
      }
      List r = null;
      switch (dataType) {
        case LONG:
          r = new COWList(dataLong, fromIndex + startIdx, subsize);
          break;
        case DOUBLE:
          r = new COWList(dataDouble, fromIndex + startIdx, subsize);
          break;
        case STRING:
          r = new COWList(dataString, fromIndex + startIdx, subsize);
          break;
        case BOOLEAN:
          r = new COWList(dataBoolean, fromIndex + startIdx, subsize);
          break;
      }
      return r;
    } else {
      return mutableCopy.subList(fromIndex, toIndex);
    }
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

  /**
   * creates a new long/double/String/boolean/Object array from the current subList (deep copy).
   * if the array provided as {@code a} is not big enough, toArray allocates and returns a new array
   */
  @Override
  public Object[] toArray(Object[] a) {
    if (readOnly) {
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
          System.arraycopy(dataString, 0, r, 0, virtualSize);
          break;
        case BOOLEAN:
          for (int i = 0; i < virtualSize; i++) {
            r[i] = (Boolean) dataBoolean.get(i + startIdx);
          }
          break;
      }
      return r;
    } else {
      return mutableCopy.toArray(a);
    }
  }

  /**
   * creates a new long/double/String/boolean array from the current subList (deep copy).
   */
  @Override
  public Object[] toArray() {
    return toArray(new Object[virtualSize]);
  }
}
