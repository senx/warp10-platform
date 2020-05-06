//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;

/**
 * A wrapper for an Object array to expose AbstractList methods.
 * The only difference with the class returned by Arrays.asList is that the toArray method does NOT return a clone
 * of the array, but the array itself.
 * This makes it possible to avoid an array copy when converting an Object[] to an ArrayList for instance:
 * // Create an Object array
 * Object[] anArray = ...
 * // The listNoCopy will directly use the anArray array (until it needs to reallocate a new one)
 * ArrayList<Object> listNoCopy = new ArrayList<Object>(new ListWrapper(anArray));
 * // The listWithCopy will use a clone of the anArray array
 * ArrayList<Object> listWithCopy = new ArrayList<Object>(Arrays.asList(anArray));
 */
public class WrapperList extends AbstractList<Object> {

  public static class ArrayItr implements Iterator<Object> {
    private int cursor;
    private final Object[] a;

    ArrayItr(Object[] a) {
      this.a = a;
    }

    @Override
    public boolean hasNext() {
      return cursor < a.length;
    }

    @Override
    public Object next() {
      int i = cursor;
      if (i >= a.length) {
        throw new NoSuchElementException();
      }
      cursor = i + 1;
      return a[i];
    }
  }

  /**
   * The array to be wrapped.
   */
  private final Object[] array;

  public WrapperList(Object[] array) {
    if (null == array) {
      throw new IllegalArgumentException("The wrapped array cannot be null");
    }

    this.array = array;
  }

  @Override
  public int size() {
    return array.length;
  }

  /**
   * Returns the wrapped array.
   *
   * @return The wrapped array, this is NOT a copy!
   */
  @Override
  public Object[] toArray() {
    return array;
  }

  /**
   * @param a
   * @return a copy of the wrapped array, in the given array if it is big enough, else in a new one.
   */
  @Override
  public <T> T[] toArray(T[] a) {
    int size = size();
    if (a.length < size) {
      return Arrays.copyOf(array, size, (Class<? extends T[]>) a.getClass());
    }
    System.arraycopy(array, 0, a, 0, size);
    if (a.length > size) {
      a[size] = null;
    }
    return a;
  }

  @Override
  public Object get(int index) {
    return array[index];
  }

  @Override
  public Object set(int index, Object element) {
    Object oldValue = array[index];
    array[index] = element;
    return oldValue;
  }

  @Override
  public int indexOf(Object o) {
    Object[] a = array;
    if (o == null) {
      for (int i = 0; i < a.length; i++) {
        if (a[i] == null) {
          return i;
        }
      }
    } else {
      for (int i = 0; i < a.length; i++) {
        if (o.equals(a[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  @Override
  public Spliterator<Object> spliterator() {
    return Spliterators.spliterator(array, Spliterator.ORDERED);
  }

  @Override
  public Iterator<Object> iterator() {
    return new ArrayItr(array);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (!(o instanceof WrapperList)) {
      return false;
    } else {
      return ((WrapperList) o).array == this.array;
    }
  }
}
