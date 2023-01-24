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

public class SubGTSAsCOWLists {

  private final int size;
  private int startIdx; // index of first element, inclusive
  private final int virtualSize; // length of the requested sub

  private final GeoTimeSerie gts;
  private final COWList ticks;
  private final COWList locations;
  private final COWList elevations;
  private final COWList values;
  private final long reference; // usually tick from where the window is drawn
  private final Object[] additiohnalInfo;

  public SubGTSAsCOWLists(GeoTimeSerie gts, int startIdx, int length, long reference, Object[] additionalInfo) {
    size = gts.size();
    this.startIdx = startIdx;
    virtualSize = length;
    this.reference = reference;
    this.additiohnalInfo = additionalInfo;
    this.gts = gts;
    this.ticks = new COWList(gts.ticks, startIdx, length);
    this.locations = new COWList(gts.locations, startIdx, length);
    this.elevations = new COWList(gts.elevations, startIdx, length);
    switch (gts.type) {
      case LONG:
        values = new COWList(gts.longValues, startIdx, length);
        break;
      case DOUBLE:
        values = new COWList(gts.doubleValues, startIdx, length);
        break;
      case STRING:
        values = new COWList(gts.booleanValues, startIdx, length);
        break;
      case BOOLEAN:
        values = new COWList(gts.stringValues, startIdx, length);
        break;
      default:
        throw new RuntimeException("Undefined GeoTimeSeries Type.");
    }
  }

  public SubGTSAsCOWLists(GeoTimeSerie gts, int startIdx, int length, long reference) {
    this(gts, startIdx, length, reference,null);
  }

  public SubGTSAsCOWLists(GeoTimeSerie gts, int startIdx, int length) {
    this(gts, startIdx, length, Long.MIN_VALUE);
  }

  public SubGTSAsCOWLists(GeoTimeSerie gts, int startIdx) {
    this(gts, startIdx, gts.size() - startIdx);
  }

  public SubGTSAsCOWLists(GeoTimeSerie gts) {
    this(gts, 0);
  }
}
