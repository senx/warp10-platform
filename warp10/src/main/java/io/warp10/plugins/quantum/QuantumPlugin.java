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
