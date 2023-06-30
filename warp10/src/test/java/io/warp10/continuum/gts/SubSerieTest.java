//
//   Copyright 2021-2023  SenX S.A.S.
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

import org.junit.Assert;
import org.junit.Test;

import io.warp10.script.aggregator.Sum;

public class SubSerieTest {

  @Test
  public void testEmpty() throws Exception {
    int n = 1000;
    GeoTimeSerie gts = new GeoTimeSerie();

    GeoTimeSerie subgts;
    subgts = GTSHelper.subSerie(gts, Long.MAX_VALUE, Long.MIN_VALUE, false);
    Assert.assertEquals(0, subgts.values);

    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    for (int i = 0; i < n; i++) {
      // Make start after stop
      subgts = GTSHelper.subSerie(gts, i + 1, i - 1, false);
      Assert.assertEquals(0, subgts.values);
    }

    subgts = GTSHelper.subSerie(gts, Long.MIN_VALUE, -1, false);
    Assert.assertEquals(0, subgts.values);
    subgts = GTSHelper.subSerie(gts, n + 1, Long.MAX_VALUE, false);
    Assert.assertEquals(0, subgts.values);

    // Test possible overflow
    subgts = GTSHelper.subSerie(gts, Long.MAX_VALUE, Long.MIN_VALUE, false);
    Assert.assertEquals(0, subgts.values);
  }

  @Test
  public void testSimple() throws Exception {
    int n = 1000;
    GeoTimeSerie gts = new GeoTimeSerie();

    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    GeoTimeSerie subgts;

    // No dups, so overwrite does not change anything
    for (boolean overwrite: new boolean[]{true, false}) {
      subgts = GTSHelper.subSerie(gts, -1, 1, overwrite);
      Assert.assertEquals(2, subgts.values);
      subgts = GTSHelper.subSerie(gts, n - 2, n, overwrite);
      Assert.assertEquals(2, subgts.values);

      for (int i = 1; i < n - 1; i++) {
        subgts = GTSHelper.subSerie(gts, i - 1, i + 1, overwrite);
        Assert.assertEquals(3, subgts.values);
      }

      for (int i = 0; i < n; i++) {
        subgts = GTSHelper.subSerie(gts, 0, i, overwrite);
        Assert.assertEquals((i + 1), subgts.values);
        subgts = GTSHelper.subSerie(gts, i, n, overwrite);
        Assert.assertEquals((n - i), subgts.values);
      }
    }
  }

  @Test
  public void testDuplicates() throws Exception {
    int n = 1000;
    GeoTimeSerie gts = new GeoTimeSerie();

    // Use 3 loops to test subSerie sorting
    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }
    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }
    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    //
    // overwrite false
    //

    GeoTimeSerie subgts;

    subgts = GTSHelper.subSerie(gts, -1, 1, false);
    Assert.assertEquals(6, subgts.values);
    subgts = GTSHelper.subSerie(gts, n - 2, n, false);
    Assert.assertEquals(6, subgts.values);

    for (int i = 1; i < n - 1; i++) {
      subgts = GTSHelper.subSerie(gts, i, i, false);
      Assert.assertEquals(3, subgts.values);
      subgts = GTSHelper.subSerie(gts, i - 1, i + 1, false);
      Assert.assertEquals(9, subgts.values);
    }

    //
    // overwrite true
    //

    subgts = GTSHelper.subSerie(gts, -1, 1, true);
    Assert.assertEquals(2, subgts.values);
    subgts = GTSHelper.subSerie(gts, n - 2, n, true);
    Assert.assertEquals(2, subgts.values);

    for (int i = 1; i < n - 1; i++) {
      subgts = GTSHelper.subSerie(gts, i, i, true);
      Assert.assertEquals(1, subgts.values);
      subgts = GTSHelper.subSerie(gts, i - 1, i + 1, true);
      Assert.assertEquals(3, subgts.values);
    }
  }

  @Test
  public void testNotPresent() throws Exception {
    int n = 1000 * 2;
    GeoTimeSerie gts = new GeoTimeSerie();

    // Only even ticks
    for (int i = 0; i < n; i += 2) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    GeoTimeSerie subgts;

    // No dups, so overwrite does not change anything
    for (boolean overwrite: new boolean[]{true, false}) {
      subgts = GTSHelper.subSerie(gts, -1, 1, overwrite);
      Assert.assertEquals(1, subgts.values);
      subgts = GTSHelper.subSerie(gts, n - 2, n, overwrite);
      Assert.assertEquals(1, subgts.values);

      for (int i = 1; i < n - 1; i++) {
        subgts = GTSHelper.subSerie(gts, i - 1, i + 1, overwrite);
        Assert.assertEquals(((0 == i % 2) ? 1 : 2), subgts.values);
      }

      for (int i = 0; i < n; i++) {
        subgts = GTSHelper.subSerie(gts, 0, i, overwrite);
        Assert.assertEquals((i / 2 + 1), subgts.values);
        subgts = GTSHelper.subSerie(gts, i, n, overwrite);
        Assert.assertEquals(((n - i) / 2), subgts.values);
      }
    }
  }

  @Test
  public void testMaxMinLong() throws Exception {
    GeoTimeSerie gts = new GeoTimeSerie();

    GTSHelper.setValue(gts, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, false);
    GTSHelper.setValue(gts, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, false);
    GTSHelper.setValue(gts, 0, 0, 0, 0, false);

    GeoTimeSerie subgts;

    // No dups, so overwrite does not change anything
    for (boolean overwrite: new boolean[]{true, false}) {
      subgts = GTSHelper.subSerie(gts, Long.MIN_VALUE, Long.MIN_VALUE, overwrite);
      Assert.assertEquals(1, subgts.values);
      subgts = GTSHelper.subSerie(gts, Long.MAX_VALUE, Long.MAX_VALUE, overwrite);
      Assert.assertEquals(1, subgts.values);
      subgts = GTSHelper.subSerie(gts, Long.MIN_VALUE, Long.MAX_VALUE, overwrite);
      Assert.assertEquals(3, subgts.values);
    }

    // Add duplicates
    GTSHelper.setValue(gts, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, false);
    GTSHelper.setValue(gts, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, false);

    subgts = GTSHelper.subSerie(gts, Long.MIN_VALUE, Long.MIN_VALUE, false);
    Assert.assertEquals(2, subgts.values);
    subgts = GTSHelper.subSerie(gts, Long.MAX_VALUE, Long.MAX_VALUE, false);
    Assert.assertEquals(2, subgts.values);
    subgts = GTSHelper.subSerie(gts, Long.MIN_VALUE, Long.MAX_VALUE, false);
    Assert.assertEquals(5, subgts.values);
  }

  @Test
  public void testBucketizedSimple() throws Exception {
    int n = 1000;
    GeoTimeSerie gts = new GeoTimeSerie();

    for (int i = 0; i < n; i++) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    gts = GTSHelper.bucketize(gts, 1, n, n - 1, new Sum(" bucketizer.sum", false), n);

    GeoTimeSerie subgts;

    // No dups, so overwrite does not change anything
    for (boolean overwrite: new boolean[]{true, false}) {
      subgts = GTSHelper.subSerie(gts, -1, 1, overwrite);
      Assert.assertEquals(2, subgts.values);
      subgts = GTSHelper.subSerie(gts, n - 2, n, overwrite);
      Assert.assertEquals(2, subgts.values);

      for (int i = 1; i < n - 1; i++) {
        subgts = GTSHelper.subSerie(gts, i - 1, i + 1, overwrite);
        Assert.assertEquals(3, subgts.values);
      }

      for (int i = 0; i < n; i++) {
        subgts = GTSHelper.subSerie(gts, 0, i, overwrite);
        Assert.assertEquals((i + 1), subgts.values);
        subgts = GTSHelper.subSerie(gts, i, n, overwrite);
        Assert.assertEquals((n - i), subgts.values);
      }
    }
  }

  @Test
  public void testBucketizedEmptyBuckets() throws Exception {
    int n = 1000 * 2;
    GeoTimeSerie gts = new GeoTimeSerie();

    // Only even ticks
    for (int i = 0; i < n; i += 2) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    gts = GTSHelper.bucketize(gts, 1, n, n - 1, new Sum(" bucketizer.sum", false), n);

    GeoTimeSerie subgts;

    // No dups, so overwrite does not change anything
    for (boolean overwrite: new boolean[]{true, false}) {
      subgts = GTSHelper.subSerie(gts, -1, 1, overwrite);
      Assert.assertEquals(1, subgts.values);
      subgts = GTSHelper.subSerie(gts, n - 2, n, overwrite);
      Assert.assertEquals(1, subgts.values);

      for (int i = 1; i < n - 1; i++) {
        subgts = GTSHelper.subSerie(gts, i - 1, i + 1, overwrite);
        Assert.assertEquals(((0 == i % 2) ? 1 : 2), subgts.values);
      }

      for (int i = 0; i < n; i++) {
        subgts = GTSHelper.subSerie(gts, 0, i, overwrite);
        Assert.assertEquals((i / 2 + 1), subgts.values);
        subgts = GTSHelper.subSerie(gts, i, n, overwrite);
        Assert.assertEquals(((n - i) / 2), subgts.values);
      }
    }
  }

  @Test
  public void testBucketizedSuppTicks() throws Exception {
    int n = 1000 * 4;
    GeoTimeSerie gts = new GeoTimeSerie();

    // Only even ticks multiple of 4
    for (int i = 0; i < n; i += 4) {
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    gts = GTSHelper.bucketize(gts, 2, n / 2, n - 4, new Sum(" bucketizer.sum", false), n / 2);

    GeoTimeSerie subgts;

    // Add ticks to the bucketized GTS
    // Only odd ticks
    for (int i = 1; i < n - 4; i += 2) {
      GTSHelper.setValue(gts, i, i, i, i, false);
      // Add dups
      GTSHelper.setValue(gts, i, i, i, i, false);
    }

    subgts = GTSHelper.subSerie(gts, -1, 1, false);
    Assert.assertEquals(3, subgts.values);
    subgts = GTSHelper.subSerie(gts, n - 5, n - 3, false);
    Assert.assertEquals(3, subgts.values);

    for (int i = 1; i < n - 4; i++) {
      subgts = GTSHelper.subSerie(gts, i - 1, i + 1, false);
      if (i % 4 == 0) {
        Assert.assertEquals(5, subgts.values);
      } else if (i % 2 == 0) {
        Assert.assertEquals(4, subgts.values);
      } else {
        Assert.assertEquals(3, subgts.values);
      }
    }
  }
}
