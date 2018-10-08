package io.warp10.plugins.udp;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import io.warp10.script.WarpScriptLib;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

public class UDPWarp10Plugin extends AbstractWarp10Plugin implements Runnable {
  
  /**
   * Directory where spec files are located
   */
  private static final String CONF_UDP_DIR = "udp.dir";
  
  /**
   * Period at which to scan the spec directory
   */
  private static final String CONF_UDP_PERIOD = "udp.period";  

  /**
   * Default scanning period in ms
   */
  private static final long DEFAULT_PERIOD = 60000L;
  
  private String dir;
  private long period;

  /**
   * Map of spec file to UDPConsumer instance
   */
  private Map<String,UDPConsumer> consumers = new HashMap<String,UDPConsumer>();
  
  private boolean done = false;
  
  public UDPWarp10Plugin() {
    super();
  }
  
  @Override
  public void run() {
    while(true) {
      
      DirectoryStream<Path> pathes = null;
      
      try {
        
        if (done) {
          return;
        }
        
        pathes = Files.newDirectoryStream(new File(dir).toPath(), "*.mc2");
        
        Iterator<Path> iter = pathes.iterator();
        
        Set<String> specs = new HashSet<String>();
        
        while (iter.hasNext()) {
          Path p = iter.next();
          
          String filename = p.getFileName().toString();
          
          boolean load = false;
          
          if (this.consumers.containsKey(filename)) {
            if (this.consumers.get(filename).getWarpScript().length() != p.toFile().length()) {
              load = true;
            }
          } else {
            // This is a new spec
            load = true;
          }
          
          if (load) {
            load(filename);
          }
          specs.add(filename);
        }
                
        //
        // Clean the specs which disappeared
        //
        
        Set<String> removed = new HashSet<String>(this.consumers.keySet());
        removed.removeAll(specs);
        
        for (String spec: removed) {
          try {
            consumers.remove(spec).end();
          } catch (Exception e) {              
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        if (null != pathes) {
          try { pathes.close(); } catch (IOException ioe) {}
        }
      }
      
      LockSupport.parkNanos(this.period * 1000000L);
    }
  }
  
  /**
   * Load a spec file
   * @param filename
   */
  private boolean load(String filename) {
    
    //
    // Stop the current UDPConsumer if it exists
    //
    
    UDPConsumer consumer = consumers.get(filename);
    
    if (null != consumer) {
      consumer.end();
    }
    
    try {
      consumer = new UDPConsumer(new File(this.dir, filename).toPath());
    } catch (Exception e) {
      return false;
    }
    
    consumers.put(filename, consumer);
    
    return true;
  }
  
  @Override
  public void init(Properties properties) {
    this.dir = properties.getProperty(CONF_UDP_DIR);
    
    if (null == this.dir) {
      throw new RuntimeException("Missing '" + CONF_UDP_DIR + "' configuration.");
    }
    
    this.period = Long.parseLong(properties.getProperty(CONF_UDP_PERIOD, Long.toString(DEFAULT_PERIOD)));
    
    //
    // Register shutdown hook
    //
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        done = true;
        System.out.println("UDP Plugin shutting down all consumers.");
        this.interrupt();
        for (UDPConsumer consumer: consumers.values()) {
          try {
            consumer.end();
          } catch (Exception e) {            
          }
        }
      }
    });
    
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("[Warp 10 UDP Plugin " + this.dir + "]");
    t.start();
  }
}
