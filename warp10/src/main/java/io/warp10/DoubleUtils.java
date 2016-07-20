package io.warp10;

import sun.misc.DoubleConsts;

public class DoubleUtils {
  public static boolean isFinite(double d) {
    return Math.abs(d) <= DoubleConsts.MAX_VALUE;
  }
}
