package io.warp10;

import sun.misc.FloatConsts;

public class FloatUtils {
  public static boolean isFinite(float f) {
    return Math.abs(f) <= FloatConsts.MAX_VALUE;
  }
}
