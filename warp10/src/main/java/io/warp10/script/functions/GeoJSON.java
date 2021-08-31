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
import org.wololo.jts2geojson.GeoJSONReader;

/**
 * Converts a Geo JSON Text String into a GeoXP Shape suitable for geo filtering
 */
public class GeoJSON extends GeoImporter {

  public GeoJSON(String name, boolean uniform) {
    super(name, uniform, String.class, "a GeoJSON " + TYPEOF.TYPE_STRING);
  }

  @Override
  public Geometry convert(Object input) throws Exception {
    return geoJSONToGeometry((String) input);
  }

  public static Geometry geoJSONToGeometry(String geoJson) {
    GeoJSONReader reader = new GeoJSONReader();
    return reader.read(geoJson);
  }
}
