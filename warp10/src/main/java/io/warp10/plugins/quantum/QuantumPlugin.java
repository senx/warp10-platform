//
//   Copyright 2016  Cityzen Data
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

package io.warp10.plugins.quantum;

import io.warp10.warp.sdk.AbstractWarp10Plugin;

import java.lang.reflect.Method;
import java.util.Properties;

public class QuantumPlugin extends AbstractWarp10Plugin implements Runnable {

  private static final String QUANTUM_MAIN_CLASS = "io.warp10.quantum.Main";
  private static final String CONF_QUANTUM_HOST = "quantum.host";
  private static final String CONF_QUANTUM_PORT = "quantum.port";

  private Properties properties = null;

  @Override
  public void init(Properties properties) {
    String host = properties.getProperty(CONF_QUANTUM_HOST);
    String port = properties.getProperty(CONF_QUANTUM_PORT);

    this.properties = properties;

    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("[QuantumPlugin " + host + ":" + port + "]");
    t.start();
  }

  @Override
  public void run() {

    try {
      Class<?> cls = this.getClass().getClassLoader().loadClass(QUANTUM_MAIN_CLASS);
      Object main = cls.newInstance();
      Method meth = cls.getMethod("init", Properties.class);
      meth.invoke(main, this.properties);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
