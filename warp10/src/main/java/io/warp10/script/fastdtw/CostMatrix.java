/*
 * CostMatrix.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

interface CostMatrix {
  public void put(int col, int row, double value);

  public double get(int col, int row);

  public int size();

} // end interface CostMatrix
