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
import com.vividsolutions.jts.io.gml2.GMLHandler;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;

public class GeoKML extends GeoImporter {

  private static class KMLHandler extends GMLHandler {

    public KMLHandler() {
      super(new GeometryFactory(), null);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      // GML tags start with gml: while KML tags have no such prefix.
      super.startElement(uri, localName, "gml:" + qName, attributes);
    }
  }

  public GeoKML(String name, boolean uniform) {
    super(name, uniform, String.class, "a KML " + TYPEOF.TYPE_STRING);
  }

  @Override
  protected Geometry convert(Object input) throws Exception {
    return KMLToGeometry((String) input);
  }

  public static Geometry KMLToGeometry(String kml) throws ParserConfigurationException, SAXException, IOException {
    SAXParserFactory fact = SAXParserFactory.newInstance();

    fact.setNamespaceAware(false);
    fact.setValidating(false);

    SAXParser parser = fact.newSAXParser();

    KMLHandler kh = new KMLHandler();
    parser.parse(new InputSource(new StringReader(kml)), kh);

    return kh.getGeometry();
  }
}
