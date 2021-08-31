//
//   Copyright 2021  SenX S.A.S.
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
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.gml2.GMLReader;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class GeoGML extends GeoImporter {

  public GeoGML(String name, boolean uniform) {
    super(name, uniform, String.class, "a GML " + TYPEOF.TYPE_STRING);
  }

  @Override
  protected Geometry convert(Object input) throws Exception {
    return GMLToGeometry((String) input);
  }

  public static Geometry GMLToGeometry(String gml) throws IOException, ParserConfigurationException, SAXException {
    GMLReader reader = new GMLReader();
    return reader.read(gml, new GeometryFactory());
  }
}
