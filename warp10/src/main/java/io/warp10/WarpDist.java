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

package io.warp10;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.KafkaWebCallBroker;
import io.warp10.continuum.KafkaWebCallService;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.Egress;
import io.warp10.continuum.geo.GeoDirectory;
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
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.script.WarpScriptJarRepository;
import io.warp10.script.WarpScriptMacroRepository;
import io.warp10.script.ScriptRunner;
import io.warp10.sensision.Sensision;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    Configuration.CONTINUUM_HASH_INDEX,
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
   * Do we run an 'egress' service. Used in EinsteinMacroRepository to bail out if not
   */
  private static boolean hasEgress = false;
  
  public static void setProperties(Properties props) {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }
    
    properties = props;
  }
  
  public static void setProperties(String file) throws IOException {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }
        
    if (null != file) {
      properties = readConfig(new FileInputStream(file), null);
    } else {
      properties = readConfig(new StringReader(""), null);
    }

    //
    // Force a call to Constants.TIME_UNITS_PER_MS to check timeunits value is correct
    //
    long warpTimeunits = Constants.TIME_UNITS_PER_MS;

    //
    // Load tokens from file
    //
    
    //if (null != properties.getProperty(CONTINUUM_TOKEN_FILE)) {
    Tokens.init(properties.getProperty(Configuration.WARP_TOKEN_FILE));
    //}
    
    //
    // Initialize macro repository
    //
    
    WarpScriptMacroRepository.init(properties);
    
    //
    // Initialize jar repository
    //
    
    WarpScriptJarRepository.init(properties);
  }
  
  public static void setKeyStore(KeyStore ks) {
    if (null != keystore) {
      throw new RuntimeException("KeyStore already set.");
    }
    keystore = ks.clone();
  }
  
  private static Properties readConfig(InputStream file, Properties properties) throws IOException {
    return readConfig(new InputStreamReader(file), properties);
  }
  
  static Properties readConfig(Reader reader, Properties properties) throws IOException {
    //
    // Read the properties in the config file
    //
    
    if (null == properties) {
      properties = new Properties();
    }
    
    BufferedReader br = new BufferedReader(reader);
    
    int lineno = 0;
    
    int errorcount = 0;
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      line = line.trim();
      lineno++;
      
      // Skip comments and blank lines
      if ("".equals(line) || line.startsWith("//") || line.startsWith("#") || line.startsWith("--")) {
        continue;
      }
      
      // Lines not containing an '=' will emit warnings
      
      if (!line.contains("=")) {
        System.err.println("Line " + lineno + " is missing an '=' sign, skipping.");
        continue;
      }
      
      String[] tokens = line.split("=");
      
      if (tokens.length > 2) {
        System.err.println("Invalid syntax on line " + lineno + ", will force an abort.");
        errorcount++;
        continue;
      }
      
      if (tokens.length < 2) {
        System.err.println("Empty value for property '" + tokens[0] + "', ignoring.");
        continue;
      }

      // Remove URL encoding if a '%' sign is present in the token
      for (int i = 0; i < tokens.length; i++) {
        if (tokens[i].contains("%")) {
          tokens[i] = URLDecoder.decode(tokens[i], "UTF-8");
        }
        tokens[i] = tokens[i].trim();
      }
      
      //
      // Ignore empty properties
      //
      
      if ("".equals(tokens[1])) {
        continue;
      }
      
      //
      // Set property
      //
      
      properties.setProperty(tokens[0], tokens[1]);
    }
    
    br.close();
    
    if (errorcount > 0) {
      System.err.println("Aborting due to " + errorcount + " error" + (errorcount > 1 ? "s" : "") + ".");
      System.exit(-1);
    }
    
    //
    // Now override properties with system properties
    //

    Properties sysprops = System.getProperties();

    for (Entry<Object, Object> entry : sysprops.entrySet()) {
      String name = entry.getKey().toString();
      String value = entry.getValue().toString();

      // URL Decode name/value if needed
      if (name.contains("%")) {
        name = URLDecoder.decode(name, "UTF-8");
      }
      if (value.contains("%")) {
        value = URLDecoder.decode(value, "UTF-8");
      }

      // Override property
      properties.setProperty(name, value);
    }
    
    //
    // Now expand ${xxx} contstructs
    //
    
    Pattern VAR = Pattern.compile(".*\\$\\{([^}]+)\\}.*");
    
    Set<String> emptyProperties = new HashSet<String>();
    
    for (Entry<Object,Object> entry: properties.entrySet()) {
      String name = entry.getKey().toString();
      String value = entry.getValue().toString();
      
      //
      // Replace '' with the empty string
      //
      
      if ("''".equals(value)) {
        value = "";
      }
      
      int loopcount = 0;
      
      while(true) {
        Matcher m = VAR.matcher(value);
        
        if (m.matches()) {
          String var = m.group(1);
          
          if (properties.containsKey(var)) {
            value = value.replace("${" + var + "}", properties.getProperty(var));              
          } else {
            System.err.println("Property '" + var + "' referenced in property '" + name + "' is unset, unsetting '" + name + "'");
            value = null;
          }
        } else {
          break;
        }
        
        if (null == value) {
          break;
        }
        
        loopcount++;
        
        if (loopcount > 100) {
          System.err.println("Hmmm, that's embarassing, but I've been dereferencing variables " + loopcount + " times trying to set a value for '" + name + "'.");
          System.exit(-1);
        }
      }
      
      if (null == value) {
        emptyProperties.add(name);
      } else {
        properties.setProperty(name, value);
      }
    }
    
    //
    // Remove empty properties
    //
    
    for (String property: emptyProperties) {
      properties.remove(property);
    }
    
    return properties;
  }
  
  public static void main(String[] args) throws Exception {

    System.setProperty("java.awt.headless", "true");
    
    setProperties(args[0]);
        
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

    keystore.setKey(KeyStore.SIPHASH_CLASS, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_CLASS)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_CLASS).length, Configuration.WARP_HASH_CLASS + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_LABELS, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_LABELS)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_LABELS).length, Configuration.WARP_HASH_LABELS + " MUST be 128 bits long.");

    //
    // Generate secondary keys. We use the ones' complement of the primary keys
    //
    
    keystore.setKey(KeyStore.SIPHASH_CLASS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_CLASS)));
    keystore.setKey(KeyStore.SIPHASH_LABELS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_LABELS)));    
    
    keystore.setKey(KeyStore.SIPHASH_INDEX, keystore.decodeKey(properties.getProperty(Configuration.CONTINUUM_HASH_INDEX)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_INDEX).length, Configuration.CONTINUUM_HASH_INDEX + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_TOKEN, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_TOKEN)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_TOKEN).length, Configuration.WARP_HASH_TOKEN + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_APPID, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_APP)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_APPID).length, Configuration.WARP_HASH_APP + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.AES_TOKEN, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_TOKEN)));
    Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_TOKEN).length) || (24 == keystore.getKey(KeyStore.AES_TOKEN).length) || (32 == keystore.getKey(KeyStore.AES_TOKEN).length), Configuration.WARP_AES_TOKEN + " MUST be 128, 192 or 256 bits long.");
    keystore.setKey(KeyStore.AES_SECURESCRIPTS, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_SCRIPTS)));
    Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length) || (24 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length) || (32 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length), Configuration.WARP_AES_SCRIPTS + " MUST be 128, 192 or 256 bits long.");
        
    if (null != properties.getProperty(Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING)) {
      keystore.setKey(KeyStore.AES_LOGGING, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_LOGGING).length) || (24 == keystore.getKey(KeyStore.AES_LOGGING).length) || (32 == keystore.getKey(KeyStore.AES_LOGGING).length), Configuration.WARP_AES_LOGGING + " MUST be 128, 192 or 256 bits long.");      
    }
    
    if (null != properties.getProperty(Configuration.CONFIG_FETCH_PSK)) {
      keystore.setKey(KeyStore.SIPHASH_FETCH_PSK, keystore.decodeKey(properties.getProperty(Configuration.CONFIG_FETCH_PSK)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.SIPHASH_FETCH_PSK).length), Configuration.CONFIG_FETCH_PSK + " MUST be 128 bits long.");            
    }
    
    KafkaWebCallService.initKeys(keystore, properties);
        
    //
    // Initialize ThrottlingManager
    //
    
    ThrottlingManager.init();
    
    for (String subprocess: subprocesses) {
      if ("ingress".equals(subprocess)) {
        Ingress ingress = new Ingress(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "ingress");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("egress".equals(subprocess)) {
        Egress egress = new Egress(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "egress");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
        hasEgress = true;
      } else if ("store".equals(subprocess)) {
        int nthreads = Integer.valueOf(properties.getProperty(Configuration.STORE_NTHREADS));
        for (int i = 0; i < nthreads; i++) {
          //Store store = new Store(getKeyStore(), getProperties(), null);
          Store store = new Store(getKeyStore(), getProperties(), 1);
        }
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "store");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("directory".equals(subprocess)) {
        Directory directory = new Directory(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "directory");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      //} else if ("index".equals(subprocess)) {
      //  Index index = new Index(getKeyStore(), getProperties());
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
      } else if ("webcall".equals(subprocess)) {
        KafkaWebCallBroker webcall = new KafkaWebCallBroker(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "webcall");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("geodir".equals(subprocess)) {
        GeoDirectory geodir = new GeoDirectory(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "geodir");
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
}
