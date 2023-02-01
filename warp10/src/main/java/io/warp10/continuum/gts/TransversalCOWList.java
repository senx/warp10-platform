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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 *
 * Draft: this class is a WIP
 *
 * Copy on Write List implementation backed by GTS array
 * and corresponding to one field, either one of: locations, elevations, values
 *
 * This is meant to be used by REDUCE and APPLY frameworks
 */
public abstract class TransversalCOWList implements List {

  GeoTimeSerie[] gtsList;

  abstract Object[] getValues();

  /**
   * This field allows to skip values from the original data collection.
   * In particular, this is used to implement null value remover.
   */
  private int[] skippedIndices;

  public void setSkippedIndices(int[] skippedIndices) {
    this.skippedIndices = skippedIndices;
  }

  /**
   * As long as readOnly is true, the List is backed by the GTS array (view).
   * As soon as user ask for a modification of the list, data are copied in an ArrayList, and readOnly turns false.
   */
  private boolean readOnly;
  private ArrayList mutableCopy = null;

  public boolean isReadOnly() {
    return readOnly;
  }

  //
  // Todo: check following
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
