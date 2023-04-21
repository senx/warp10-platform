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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.UnaryOperator;

/**
 * Read Only List object that returns the same value for every index.
 */
public class ReadOnlyConstantList implements List {

  private final int size; // may be changed without recreating the object
  private final Object value;

  /**
   * Create a read only list that always return the same {@code value} value for every index.
   *
   * @param size List size
   */
  public ReadOnlyConstantList(int size, Object value) {

    this.value = value;
    this.size = size;
  }

  private void rangeCheck(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
    }
  }

  private String outOfBoundsMsg(int index) {
    return "Index: " + index + ", Size: " + size;
  }

  @Override
  public int indexOf(Object o) {
    if (o.equals(value) && size > 0) {
      return 0; // no null object in a primitive array
    } else {
      return -1;
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return 0 == size;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }
  
  @Override
  public Iterator iterator() {
    return new Iterator() {
      int cursor;       // index of next element to return

      public boolean hasNext() {
        return cursor != size;
      }

      public Object next() {
        int i = cursor;
        if (i >= size) {
          throw new NoSuchElementException();
        }
        Object res = value;
        cursor = i + 1;
        return res;
      }

      public void remove() {
        // not applicable
      }
    };
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
    throw new RuntimeException("This is a read only list, cannot remove elements.");
  }

  /**
   * get will return the element at index + startidx of the underlying array or bitset.
   *
   * @param index cannot be less than 0 or greater than list length
   * @return element at index + startidx
   */
  @Override
  public Object get(int index) {
    rangeCheck(index);
    return value;
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
    if (size > 0 && o.equals(value)) {
      return size - 1;
    } else {
      return -1;
    }
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
    if (subsize < 0 || subsize + toIndex > size) {
      throw new IndexOutOfBoundsException("start index(" + fromIndex + ") + length(" + subsize + ") greater than original list size(" + size + "), cannot create sublist");
    }
    return new ReadOnlyConstantList(subsize, value);
  }

  @Override
  public Spliterator spliterator() {
    return List.super.spliterator();
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
    if (r.length < size) {
      r = new Object[size];
    }
    Arrays.fill(a,value);
    return r;
  }

  /**
   * creates a new long/double/String/boolean array from the current subList (deep copy).
   */
  @Override
  public Object[] toArray() {
    return toArray(new Object[size]);
  }
}
