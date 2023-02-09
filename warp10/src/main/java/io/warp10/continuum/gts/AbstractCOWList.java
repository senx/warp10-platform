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

public abstract class AbstractCOWList implements List {

  /**
   * As long as readOnly is true, the List is a view.
   * As soon as user asks for a modification of the list, data are copied into an ArrayList, and readOnly turns false.
   */
  protected boolean readOnly = true;
  protected ArrayList mutableCopy = null;

  public boolean isReadOnly() {
    return readOnly;
  }

  protected synchronized void initialDeepCopy() {
    if (readOnly) {
      mutableCopy = new ArrayList(size());
      for (int i = 0; i < size(); i++) {
        mutableCopy.add(get(i));
      }
      readOnly = false;
    }
  }

  public abstract int size();

  protected void rangeCheck(int index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
    }
  }

  public abstract Object get(int i);

  public abstract List subList(int fromIndex, int toIndex);

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
    if (readOnly) {
      for (int i = size() - 1; i >= 0; i--) {
        if (o.equals(get(i))) {
          return i;
        }
      }
      return -1;
    } else {
      return mutableCopy.lastIndexOf(o);
    }
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
