//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.egress.Egress;
import io.warp10.continuum.ingress.Ingress;
import io.warp10.continuum.plasma.PlasmaBackEnd;
import io.warp10.continuum.plasma.PlasmaFrontEnd;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.Store;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptLib;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;

/**
 * Main class for launching components of the Continuum geo time series storage system
 */
public class WarpDist {

  //
  // We create an instance of SensisionConstants so Sensision gets its instance name set
  // before any metric or event is pushed. Otherwise registration won't take into account
  // the instance name.
  //

  static {
    SensisionConstants constant = new SensisionConstants();
  }

  private static KeyStore keystore = null;
  private static Properties properties = null;

  public static final String[] REQUIRED_PROPERTIES = {
    Configuration.WARP_COMPONENTS,
    Configuration.WARP_HASH_CLASS,
    Configuration.WARP_HASH_LABELS,
    Configuration.WARP_HASH_TOKEN,
    Configuration.WARP_HASH_APP,
    Configuration.WARP_AES_TOKEN,
    Configuration.WARP_AES_SCRIPTS,
    Configuration.CONFIG_WARPSCRIPT_UPDATE_ENDPOINT,
    Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT,
    Configuration.WARP_TIME_UNITS,
  };

  private static boolean initialized = false;

  /**
   * Do we run an 'egress' service. Used in WarpScript MacroRepository to bail out if not
   */
  private static boolean hasEgress = false;

  public static void setProperties(Properties props) {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }

    properties = props;
  }

  public static void setProperties(String[] files) throws IOException {
    try {
      WarpConfig.setProperties(false, files);

      properties = WarpConfig.getProperties();
    } catch (Throwable t) {
      System.err.println(ThrowableUtils.getErrorMessage(t));
      System.exit(-1);
    }
  }

  public static void setProperties(String file) throws IOException {
    try {
      WarpConfig.setProperties(false, file);

      properties = WarpConfig.getProperties();
    } catch (Throwable t) {
      System.err.println(ThrowableUtils.getErrorMessage(t));
      System.exit(-1);
    }
  }

  public static void setKeyStore(KeyStore ks) {
    if (null != keystore) {
      throw new RuntimeException("KeyStore already set.");
    }
    keystore = ks.clone();
  }

  public static void main(String[] args) throws Exception {

    if (null == properties) {
      System.out.println();
      System.out.println(Constants.WARP10_BANNER);
      System.out.println("  Revision " + Revision.REVISION);
      System.out.println();

      System.setProperty("java.awt.headless", "true");

      if (StandardCharsets.UTF_8 != Charset.defaultCharset()) {
        throw new RuntimeException("Default encoding MUST be UTF-8 but it is " + Charset.defaultCharset() + ". Aborting.");
      }

      if (args.length > 0) {
        setProperties(args);
      } else if (null != System.getProperty(WarpConfig.WARP10_CONFIG)) {
        setProperties(System.getProperty(WarpConfig.WARP10_CONFIG).split("[, ]+"));
      } else if (null != System.getenv(WarpConfig.WARP10_CONFIG_ENV)) {
        setProperties(System.getenv(WarpConfig.WARP10_CONFIG_ENV).split("[, ]+"));
      }
    }

    //
    // Extract components to spawn
    //

    String[] components = properties.getProperty(Configuration.WARP_COMPONENTS).split(",");

    Set<String> subprocesses = new HashSet<String>();

    for (String component: components) {
      component = component.trim();

      subprocesses.add(component);
    }

    if (properties.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    //
    // Set SIPHASH keys for class/labels/index
    //

    for (String property: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(property), "Property '" + property + "' MUST be set.");
    }

    //
    // Decode generic keys and secrets
    // We do that first so those keys do not have precedence over the specific
    // keys.
    //

    Map<String,String> secrets = new HashMap<String,String>();
    Set<Object> keys = new HashSet<Object>();
    for (Entry<Object,Object> entry: properties.entrySet()) {
      if (entry.getKey().toString().startsWith(Configuration.WARP_KEY_PREFIX)) {
        byte[] key = keystore.decodeKey(entry.getValue().toString());
        if (null == key) {
          throw new RuntimeException("Unable to decode key '" + entry.getKey() + "'.");
        }
        keystore.setKey(entry.getKey().toString().substring(Configuration.WARP_KEY_PREFIX.length()), key);
        keys.add(entry.getKey());
      } else if (entry.getKey().toString().startsWith(Configuration.WARP_SECRET_PREFIX)) {
        byte[] secret = keystore.decodeKey(entry.getValue().toString());
        if (null == secret) {
          throw new RuntimeException("Unable to decode secret '" + entry.getKey() + "'.");
        }
        // Encode secret as OPB64 and store it
        secrets.put(entry.getKey().toString().substring(Configuration.WARP_SECRET_PREFIX.length()), OrderPreservingBase64.encodeToString(secret));
        keys.add(entry.getKey());
      }
    }

    //
    // Remove keys and secrets from the properties
    //
    for (Object key: keys) {
      properties.remove(key);
    }

    // Inject the secrets
    properties.putAll(secrets);

    extractKeys(keystore, properties);

    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_FETCH_PSK, properties, Configuration.CONFIG_FETCH_PSK, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_RUNNER_PSK, properties, Configuration.RUNNER_PSK, 128, 192, 256);

    WarpScriptLib.registerExtensions();

    //
    // Initialize ThrottlingManager
    //

    ThrottlingManager.init();

    if (subprocesses.contains("egress") && subprocesses.contains("fetcher")) {
      throw new RuntimeException("'fetcher' and 'egress' cannot be specified together as components to run.");
    }

    for (String subprocess: subprocesses) {
      if ("ingress".equals(subprocess)) {
        Ingress ingress = new Ingress(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "ingress");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("egress".equals(subprocess)) {
        Egress egress = new Egress(getKeyStore(), getProperties(), false);
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "egress");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
        hasEgress = true;
      } else if ("fetcher".equals(subprocess)) {
        Egress egress = new Egress(getKeyStore(), getProperties(), true);
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "fetcher");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("store".equals(subprocess)) {
        int nthreads = Integer.valueOf(properties.getProperty(Configuration.STORE_NTHREADS));
        for (int i = 0; i < nthreads; i++) {
          Store store = new Store(getKeyStore(), getProperties(), null);
        }
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "store");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("directory".equals(subprocess)) {
        Directory directory = new Directory(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "directory");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("plasmaFE".equalsIgnoreCase(subprocess)) {
        PlasmaFrontEnd plasmaFE = new PlasmaFrontEnd(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "plasmafe");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("plasmaBE".equalsIgnoreCase(subprocess)) {
        PlasmaBackEnd plasmaBE = new PlasmaBackEnd(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "plasmabe");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("runner".equals(subprocess)) {
        ScriptRunner runner = new ScriptRunner(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "runner");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else {
        System.err.println("Unknown component '" + subprocess + "', skipping.");
        continue;
      }
    }

    // Clear master key from memory
    keystore.forget();

    //
    // Register the plugins after we've cleared the master key
    //

    AbstractWarp10Plugin.registerPlugins();

    setInitialized(true);

    //
    // We're done, let's sleep endlessly
    //

    try {
      while (true) {
        try {
          Thread.sleep(60000L);
        } catch (InterruptedException ie) {
        }
      }
    } catch (Throwable t) {
      System.err.println(t.getMessage());
    }
  }

  public static KeyStore getKeyStore() {
    if (null == keystore) {
      return null;
    }
    return keystore.clone();
  }

  public static Properties getProperties() {
    if (null == properties) {
      return null;
    }
    return (Properties) properties.clone();
  }

  public static synchronized void setInitialized(boolean initialized) {
    WarpDist.initialized = initialized;
  }

  public static synchronized boolean isInitialized() {
    return initialized;
  }

  public static boolean isEgress() {
    return hasEgress;
  }

  public static void setEgress(boolean egress) {
    hasEgress = egress;
  }

  public static void extractKeys(KeyStore keystore, Properties properties) {
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_CLASS, properties, Configuration.WARP_HASH_CLASS, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_LABELS, properties, Configuration.WARP_HASH_LABELS, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_TOKEN, properties, Configuration.WARP_HASH_TOKEN, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_APPID, properties, Configuration.WARP_HASH_APP, 128);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_TOKEN, properties, Configuration.WARP_AES_TOKEN, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_SECURESCRIPTS, properties, Configuration.WARP_AES_SCRIPTS, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_METASETS, properties, Configuration.WARP_AES_METASETS, 128, 192, 256);
    KeyStore.checkAndSetKey(keystore, KeyStore.AES_LOGGING, properties, Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING, 128, 192, 256);

    // Generate secondary keys. We use the ones' complement of the primary keys
    keystore.setKey(KeyStore.SIPHASH_CLASS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_CLASS)));
    keystore.setKey(KeyStore.SIPHASH_LABELS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_LABELS)));
  }
}
