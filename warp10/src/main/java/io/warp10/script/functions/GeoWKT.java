//
//   Copyright 2018-2021  SenX S.A.S.
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
import com.vividsolutions.jts.io.WKTReader;

/**
 * Converts a Well Known Text String into a GeoXP Shape suitable for geo filtering
 */
public class GeoWKT extends GeoImporter {


  public GeoWKT(String name, boolean uniform) {
    super(name, uniform, String.class, "a WKT " + TYPEOF.TYPE_STRING);
  }

  @Override
  public Geometry convert(Object input) throws Exception {
    return wktToGeometry((String) input);
  }

  public static Geometry wktToGeometry(String wkt) throws ParseException {
    WKTReader reader = new WKTReader();
    return reader.read(wkt);
  }
}
