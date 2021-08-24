//
//   Copyright 2019-2021  SenX S.A.S.
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

package io.warp10.script.functions;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

/**
 * Converts a Well Known Binary byte array into a GeoXP Shape suitable for geo filtering
 */
public class GeoWKB extends GeoImporter {

  public GeoWKB(String name, boolean uniform) {
    super(name, uniform, byte[].class, " GeoWKB " + TYPEOF.TYPE_BYTES);
  }

  @Override
  public Geometry convert(Object input) throws Exception {
    return wkbToGeometry((byte[]) input);
  }

  public static Geometry wkbToGeometry(byte[] wkb) throws ParseException {
    WKBReader reader = new WKBReader();
    return reader.read(wkb);
  }
}
