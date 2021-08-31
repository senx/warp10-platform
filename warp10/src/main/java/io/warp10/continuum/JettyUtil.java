//
//   Copyright 2018  SenX S.A.S.
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

package io.warp10.continuum;

import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;

public class JettyUtil {
  public static void setSendServerVersion(Server server, boolean send) {
    //
    // Remove display of Server header
    // @see <a href="http://stackoverflow.com/questions/15652902/remove-the-http-server-header-in-jetty-9">http://stackoverflow.com/questions/15652902/remove-the-http-server-header-in-jetty-9</a>
    //
    
    for(Connector y : server.getConnectors()) {
      for(ConnectionFactory x  : y.getConnectionFactories()) {
        if(x instanceof HttpConnectionFactory) {
          ((HttpConnectionFactory)x).getHttpConfiguration().setSendServerVersion(send);
        }
      }
    }    
  }
}
