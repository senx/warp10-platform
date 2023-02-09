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

import java.util.BitSet;
import java.util.List;

/**
 * Copy On Write List implementation backed by an existing array or bitset.
 * Data is duplicated when the list is modified.
 * The original array or bitset is never modified.
 */
public class COWList extends AbstractCOWList {

  private final int size;
  private int startIdx; // index of first element of sublist, inclusive
  private final int virtualSize; // length of the requested sublist

  private long[] dataLong = null;
  private double[] dataDouble = null;
  private String[] dataString = null;
  private BitSet dataBoolean = null;

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

  private void initialRangeCheck(int startIndex, int length) {
    if (startIndex >= size || startIndex < 0) {
      throw new IndexOutOfBoundsException("Index: " + startIndex + ", Size: " + size);
    }
    if (length < 0 || (startIndex + length) > size) {
      throw new IndexOutOfBoundsException("Start index(" + startIndex + ") + length(" + length + ") is greater than original array size(" + size + "), cannot create sublist");
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

  /**
   * If not altered, get will return the element at index + startidx of the underlying array or bitset.
   * If altered, get will return the element of the underlying ArrayList.
   *
   * @param index cannot be less than 0 or greater than list length
   * @return
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
}
