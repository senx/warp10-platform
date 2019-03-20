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
package io.warp10;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.Constants;
import io.warp10.script.WarpFleetMacroRepository;
import io.warp10.script.WarpScriptJarRepository;
import io.warp10.script.WarpScriptMacroRepository;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WarpConfig {
  
  /**
   * Name of property used in various submodules to locate the Warp 10 configuration file
   */
  public static final String WARP10_CONFIG = "warp10.config";
  
  /**
   * Name of environment variable used in various submodules to locate the Warp 10 configuration file
   */
  public static final String WARP10_CONFIG_ENV = "WARP10_CONFIG";
  
  /**
   * Name of property used at various places to define BOOTSTRAP code.
   */
  public static final String WARPSCRIPT_BOOTSTRAP = "warpscript.bootstrap";
  
  private static Properties properties = null;
  
  public static void safeSetProperties(String file) throws IOException {
    if (null != properties) {
      return;
    }
    
    if (null == file) {
      safeSetProperties((Reader) null);
    } else {      
      safeSetProperties(new FileReader(file));
    }
  }
  
  public static void setProperties(String file) throws IOException {
    if (null == file) {
      setProperties((Reader) null);
    } else {
      setProperties(new FileReader(file));
    }
  }
  
  public static void safeSetProperties(Reader reader) throws IOException {
    if (null != properties) {
      return;
    }
    
    setProperties(reader);
  }
  
  public static boolean isPropertiesSet() {
    return null != properties;
  }
  
  public static void setProperties(Reader reader) throws IOException {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }
    
    if (null != reader) {
      properties = readConfig(reader, null);
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
    
    if (null != properties.getProperty(Configuration.WARP_TOKEN_FILE)) {
      Tokens.init(properties.getProperty(Configuration.WARP_TOKEN_FILE));
    }

    //
    // Initialize macro repository
    //
    
    WarpScriptMacroRepository.init(properties);
    
    //
    // Initialize jar repository
    //
    
    WarpScriptJarRepository.init(properties);
    
    //
    // Initialize WarpFleet repository
    //
    
    WarpFleetMacroRepository.init(properties);
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

      try {
        // URL Decode name/value if needed
        if (name.contains("%")) {
          name = URLDecoder.decode(name, "UTF-8");
        }
        if (value.contains("%")) {
          value = URLDecoder.decode(value, "UTF-8");
        }

        // Override property
        properties.setProperty(name, value);        
      } catch (Exception e) {
        System.err.println("Error decoding system property '" + entry.getKey().toString() + "' = '" + entry.getValue().toString() + "', using raw values.");
        properties.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }
    }
    
    //
    // Now expand ${xxx} constructs
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
          System.err.println("Hmmm, that's embarrassing, but I've been dereferencing variables " + loopcount + " times trying to set a value for '" + name + "'.");
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

  public static Properties getProperties() {
    if (null == properties) {
      return null;
    }
    return (Properties) properties.clone();
  }
  
  public static String getProperty(String key) {
    if (null == properties) {
      throw new RuntimeException("Properties not set.");
    } else {
      return properties.getProperty(key);
    }
  }

  public static String getProperty(String key, String defaultValue) {
    if (null == properties) {
      throw new RuntimeException("Properties not set.");
    } else {
      return properties.getProperty(key, defaultValue);
    }    
  }
  
  public static Object setProperty(String key, String value) {    
    if (null == properties) {
      return null;
    } else {
      synchronized(properties) {
        // Set the new value
        if (null == value) {
          return properties.remove(key);
        } else {
          return properties.setProperty(key, value);
        }
      }
    }
  }
  
  public static void main(String... args) {
    if (2 != args.length) {
      System.err.println("2 arguments required: properties file and the property key");
      System.exit(-1);
    }

    if (null != properties) {
      System.err.println("Properties already set");
      System.exit(-1);
    }

    String file = args[0];
    String key = args[1];
    try {
      properties = WarpConfig.readConfig(new FileReader(file), null);
      System.out.println(key + "=" + WarpConfig.getProperty(key));
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
