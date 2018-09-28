package io.warp10.warp.sdk;

import io.warp10.WarpClassLoader;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.WarpScriptLib;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic interface for Warp 10 plugins
 */
public abstract class AbstractWarp10Plugin {
  
  private static Logger LOG = LoggerFactory.getLogger(AbstractWarp10Plugin.class);
  
  private static final AtomicBoolean registered = new AtomicBoolean(false);
  /**
   * Method called to initialize the plugin
   * 
   * @param properties Warp 10 configuration properties
   */
  public abstract void init(Properties properties);  
  
  public static final void registerPlugins() { 
    
    if (registered.get()) {
      return;
    }
    
    registered.set(true);
    
    Properties props = WarpConfig.getProperties();
    
    Set<String> plugs = new HashSet<String>();
    
    if (null != props && props.containsKey(Configuration.WARP10_PLUGINS)) {
      String[] plugins = props.getProperty(Configuration.WARP10_PLUGINS).split(",");
           
      for (String plugin: plugins) {
        plugs.add(plugin.trim());
      }
    }

    for (Object key: props.keySet()) {
      if (key.toString().startsWith(Configuration.WARP10_PLUGIN_PREFIX)) {
        plugs.add(props.getProperty(key.toString()).toString().trim());
      }
    }
    
    if (plugs.isEmpty()) {
      return;
    }
    
    boolean failedPlugin = false;
      
    //
    // Determine the possible jar from which we were loaded
    //
      
    String wsljar = null;
    URL wslurl = AbstractWarp10Plugin.class.getResource('/' + AbstractWarp10Plugin.class.getCanonicalName().replace('.',  '/') + ".class");
    if (null != wslurl && "jar".equals(wslurl.getProtocol())) {
      wsljar = wslurl.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");
    }
      
    for (String plugin: plugs) {
      try {
        //
        // Locate the class using the current class loader
        //
        
        URL url = AbstractWarp10Plugin.class.getResource('/' + plugin.replace('.', '/') + ".class");
        
        if (null == url) {
          LOG.error("Unable to load plugin '" + plugin + "', make sure it is in the class path.");
          failedPlugin = true;
          continue;
        }
        
        Class cls = null;

        //
        // If the class was located in a jar, load it using a specific class loader
        // so we can have fat jars with specific deps, unless the jar is the same as
        // the one from which AbstractWarp10Plugin was loaded, in which case we use the same
        // class loader.
        //
        
        if ("jar".equals(url.getProtocol())) {
          String jarfile = url.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");

          ClassLoader cl = AbstractWarp10Plugin.class.getClassLoader();
          
          // If the jar differs from that from which AbstractWarp10Plugin was loaded, create a dedicated class loader
          if (!jarfile.equals(wsljar)) {
            cl = new WarpClassLoader(jarfile, AbstractWarp10Plugin.class.getClassLoader());
          }
        
          cls = Class.forName(plugin, true, cl);
        } else {
          cls = Class.forName(plugin, true, AbstractWarp10Plugin.class.getClassLoader());
        }

        AbstractWarp10Plugin wse = (AbstractWarp10Plugin) cls.newInstance();          
        wse.init(WarpConfig.getProperties());
        System.out.println("LOADED plugin '" + plugin  + "'");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
      
    if (failedPlugin) {
      throw new RuntimeException("Some WarpScript plugins could not be loaded, aborting.");
    }
  }
  
  /**
   * Retrieve the exposed StoreClient. We use introspection to avoid having to
   * include EgressExecHandler in the WarpScript lib jar.
   * 
   * @return The exposed StoreClient or null
   */
  public static final StoreClient getExposedStoreClient() {
    try {
      Class egress = Class.forName("io.warp10.continuum.egress.EgressExecHandler");
      Method m = egress.getDeclaredMethod("getExposedStoreClient", new Class[0]);
      return (StoreClient) m.invoke(null, new Object[0]);
    } catch (Throwable t) {
      t.printStackTrace();
    }
    return null;
  }
  
  /**
   * Retrieve the exposed DirectoryClient. We use introspection to avoid having to
   * include EgressExecHandler in the WarpScript lib jar.
   * 
   * @return The exposed DirectoryClient or null
   */
  public static final DirectoryClient getExposedDirectoryClient() {
    try {
      Class egress = Class.forName("io.warp10.continuum.egress.EgressExecHandler");
      Method m = egress.getDeclaredMethod("getExposedDirectoryClient", new Class[0]);
      return (DirectoryClient) m.invoke(null, new Object[0]);
    } catch (Throwable t) {
      t.printStackTrace();
    }
    return null;
  }
}
