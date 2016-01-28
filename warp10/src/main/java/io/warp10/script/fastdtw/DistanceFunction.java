/*
 * Arrays.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

import io.warp10.continuum.gts.GeoTimeSerie;

public interface DistanceFunction {
  public double calcDistance(GeoTimeSerie x, int i, GeoTimeSerie y, int j);
}
