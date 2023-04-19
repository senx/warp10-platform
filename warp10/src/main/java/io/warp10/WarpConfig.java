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

import com.google.common.annotations.Beta;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.Constants;
import io.warp10.script.WarpFleetMacroRepository;
import io.warp10.script.WarpScriptMacroRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WarpConfig {

  private static final Logger LOG = LoggerFactory.getLogger(WarpConfig.class);

  /**
   * Name of property used in various submodules to locate the Warp 10 configuration file
   */
  public static final String WARP10_CONFIG = "warp10.config";

  private static final String WARP10_NOENV = "warp10.noenv";
  private static final String WARP10_NOSYS = "warp10.nosys";
  private static final String WARP10_IGNOREFAILEDEXPANDS  = "warp10.ignorefailedexpands";

  /**
   * Name of environment variable used in various submodules to locate the Warp 10 configuration file
   */
  public static final String WARP10_CONFIG_ENV = "WARP10_CONFIG";

  /**
   * Name of property used at various places to define BOOTSTRAP code.
   */
  public static final String WARPSCRIPT_BOOTSTRAP = "warpscript.bootstrap";

  private static Properties properties = null;

  public static final String THREAD_PROPERTY_SESSION = ".session";

  public static final String THREAD_PROPERTY_TOKEN = ".token";

  /**
   * The concept of thread properties is to allocate a per thread map which can contain
   * arbitrary elements keyed by STRINGs and therefore accessible to all methods invoked within
   * the same thread.
   * One of the use cases is to be able to know that multiple calls to a given method are performed
   * within the same thread. This is useful in ValueEncoder instances for example to keep some context
   * when parsing multiple lines independently but within the same 'session'.
   * The per thread property map should be cleared at the end of the main Warp 10 entry points (/update, /exec) and
   * at the end of any additional entry points defined.
   * Those endpoints should set the '.session' thread property at the beginning of a try block with a finally clause
   * calling clearThreadProperties.
   * Of course if the convention above is not respected, property maps might be created without clearing ensured.
   *
   */

  @Beta
  private static final ThreadLocal<Map<String,Object>> threadProperties = new ThreadLocal<Map<String,Object>>() {
    protected java.util.Map<String,Object> initialValue() {
      return new HashMap<String,Object>();
    }
  };

  @Beta
  public static Object getThreadProperty(String key) {
    return threadProperties.get().get(key);
  }

  @Beta
  public static Object setThreadProperty(String key, Object value) {
    return threadProperties.get().put(key, value);
  }

  @Beta
  public static Object removeThreadProperty(String key) {
    Map<String,Object> props = threadProperties.get();
    Object previous;
    previous = props.remove(key);

    // If the property map is empty, clear it
    if (0 == props.size()) {
      threadProperties.remove();
    }
    return previous;
  }

  @Beta
  public static void clearThreadProperties() {
    threadProperties.remove();
  }

  public static void safeSetProperties(String... files) throws IOException {
    safeSetProperties(true, files);
  }

  public static void safeSetProperties(boolean exitOnError, String... files) throws IOException {
    if (null != properties) {
      return;
    }

    setProperties(exitOnError, files);
  }

  public static void setProperties(String... files) throws IOException {
    setProperties(true, files);
  }

  public static void setProperties(boolean exitOnError, String... files) throws IOException {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }

    properties = new Properties();

    if (null != files && 0 != files.length) {
      //
      // Read all files, in the order they were provided.
      // If a file starts with '@', treat it as a file containing lists of files
      // and add its content to the list of files. This cannot be recursive to avoid infinite loops.
      //

      List<String> filenames = new ArrayList<String>(Arrays.asList(files));

      ListIterator<String> iterator = filenames.listIterator();

      while (iterator.hasNext()) {
        String file = iterator.next();

        if (null != file && 0 < file.length() && '@' == file.charAt(0)) {
          // Remove the @file from the filenames list.
          iterator.remove();

          try (BufferedReader br = new BufferedReader(new FileReader(file.substring(1)))) {
            String line;
            while ((line = br.readLine()) != null) {
              iterator.add(line);
            }
          }
        }
      }

      for (String filename: filenames) {
        if (null != filename) {
          File file = new File(filename);
          try (InputStream fileInputStreamStream = new FileInputStream(file)) {
            readConfig(exitOnError, fileInputStreamStream, properties);
          } catch (IOException ioe) {
            throw new IOException("Found errors while reading " + filename + ".", ioe);
          }
        }
      }
    }

    envVarsAndSysPropsOverride();
    expandVars(exitOnError);
    checkAndInit();
  }

  public static void safeSetProperties(Reader reader) throws IOException {
    safeSetProperties(true, reader);
  }

  public static void safeSetProperties(boolean exitOnError, Reader reader) throws IOException {
    if (null != properties) {
      return;
    }

    setProperties(exitOnError, reader);
  }

  public static boolean isPropertiesSet() {
    return null != properties;
  }

  public static void setProperties(Reader reader) throws IOException {
    setProperties(true, reader);
  }

  public static void setProperties(boolean exitOnError, Reader reader) throws IOException {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }

    if (null != reader) {
      properties = readConfig(exitOnError, reader, null);
    } else {
      properties = readConfig(exitOnError, new StringReader(""), null);
    }

    envVarsAndSysPropsOverride();
    expandVars(exitOnError);
    checkAndInit();
  }

  private static void checkAndInit(){
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
    // Initialize WarpFleet repository
    //

    WarpFleetMacroRepository.init(properties);
  }

  private static Properties readConfig(boolean exitOnError, InputStream file, Properties properties) throws IOException {
    return readConfig(exitOnError, new InputStreamReader(file), properties);
  }

  public static Properties readConfig(Reader reader, Properties properties) throws IOException {
    return readConfig(true, reader, properties);
  }

  public static Properties readConfig(boolean exitOnError, Reader reader, Properties properties) throws IOException {
    //
    // Read the properties in the config file
    //

    if (null == properties) {
      properties = new Properties();
    }

    BufferedReader br = new BufferedReader(reader);

    int lineno = 0;

    List<Integer> linesInError = new ArrayList<Integer>();

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
        LOG.warn("'" + line + "' on line " + lineno + " is missing an '=' sign, skipping.");
        continue;
      }

      String[] tokens = line.split("=");

      if (tokens.length > 2) {
        linesInError.add(lineno);
        continue;
      }

      if (tokens.length < 2) {
        LOG.warn("Empty value for property '" + tokens[0] + "' on line " + lineno + ", ignoring.");
        continue;
      }

      // Remove URL encoding if a '%' sign is present in the token
      try {
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = WarpURLDecoder.decode(tokens[i], StandardCharsets.UTF_8);
          tokens[i] = tokens[i].trim();
        }
      } catch (IllegalArgumentException iae) {
        linesInError.add(lineno);
        continue;
      }

      //
      // Remove properties if value is -
      //

      if ("-".equals(tokens[1])) {
        properties.remove(tokens[0]);
        continue;
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

    if (!linesInError.isEmpty()) {
      if (exitOnError) {
        LOG.error("Malformed lines " + linesInError.toString() + " in configuration.");
        System.exit(-1);
      } else {
        throw new IOException("Malformed lines " + linesInError.toString() + ".");
      }
    }

    return properties;
  }

  private static void envVarsAndSysPropsOverride() throws IOException {

    //
    // Override properties with environment variables
    //

    if (null == properties.getOrDefault(WARP10_NOENV, System.getProperty(WARP10_NOENV))) {
      for (Entry<String, String> entry : System.getenv().entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();

        try {
          // URL Decode name/value if needed
          name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);
          value = WarpURLDecoder.decode(value, StandardCharsets.UTF_8);

          // Override property
          properties.setProperty(name, value);
        } catch (Exception e) {
          LOG.warn("Failed to decode environment variable '" + entry.getKey() + "' = '" + entry.getValue() + "', using raw value.");
          properties.setProperty(entry.getKey(), entry.getValue());
        }
      }
    }

    //
    // Override properties with system properties
    //

    if (null == properties.getOrDefault(WARP10_NOSYS, System.getProperty(WARP10_NOSYS))) {
      Properties sysprops = System.getProperties();

      for (Entry<Object, Object> entry : sysprops.entrySet()) {
        String name = entry.getKey().toString();
        String value = entry.getValue().toString();

        try {
          // URL Decode name/value if needed
          name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);
          value = WarpURLDecoder.decode(value, StandardCharsets.UTF_8);

          // Override property
          properties.setProperty(name, value);
        } catch (Exception e) {
          LOG.warn("Error decoding system property '" + entry.getKey().toString() + "' = '" + entry.getValue().toString() + "', using raw values.");
          properties.setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
      }
    }
  }

  private static void expandVars(boolean exitOnError) throws IOException {
    //
    // Now expand ${xxx} constructs
    //

    Pattern VAR = Pattern.compile(".*\\$\\{([^}]+)\\}.*");

    Set<String> emptyProperties = new HashSet<String>();

    boolean ignoreFailedExpands = null != properties.getOrDefault(WARP10_IGNOREFAILEDEXPANDS, System.getProperty(WARP10_IGNOREFAILEDEXPANDS));

    for (Entry<Object, Object> entry : properties.entrySet()) {
      String name = entry.getKey().toString();
      String value = entry.getValue().toString();

      //
      // Replace '' with the empty string
      //

      if ("''".equals(value)) {
        value = "";
      }

      int loopcount = 0;

      String origValue = value;

      while (true) {
        Matcher m = VAR.matcher(value);

        if (m.matches()) {
          String var = m.group(1);

          if (properties.containsKey(var)) {
            value = value.replace("${" + var + "}", properties.getProperty(var));
          } else {
            LOG.warn("Property '" + var + "' referenced in property '" + name + "' is unset, unsetting '" + name + "'");
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
          LOG.warn("Hmmm, that's embarrassing, but I've been dereferencing variables " + loopcount + " times trying to set a value for '" + name + "' from value '" + origValue + "'.");
          if (ignoreFailedExpands) {
            LOG.warn("Removing property '" + name + "'.");
            // Clearing the value
            value = null;
            break;
          } else {
            if (exitOnError) {
              LOG.error("Failed to expand '" + name + "' in configuration.");
              System.exit(-1);
            } else {
              throw new IOException("Failed to expand '" + name + "'.");
            }
          }
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

    for (String property : emptyProperties) {
      properties.remove(property);
    }
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
      synchronized (properties) {
        // Set the new value
        if (null == value) {
          return properties.remove(key);
        } else {
          return properties.setProperty(key, value);
        }
      }
    }
  }

  /*
   * Adapt the java.version system property so we stick to the previous versions format of 1.major.minor
   * see https://openjdk.java.net/jeps/223
   */
  public static String getOriginalFormatJavaVersion() {
    String jversion = System.getProperty("java.version");

    if (properties.containsKey(Configuration.WARP_JAVA_VERSION)) {
      jversion = properties.getProperty(Configuration.WARP_JAVA_VERSION);
    } else {
      if (jversion.startsWith("1.")) {
        // Check that we have two colons, if not add ".0"
        if (-1 == jversion.indexOf(".", 2)) {
          jversion = jversion + ".0";
        }
      } else if (jversion.contains(".")) {
        // version contains a dot, it's propably of the form
        // x.y.z-www or x.y.z+www, so check if it has two dots
        // and then prepend "1."
        if (-1 != jversion.indexOf(".", jversion.indexOf(".") + 1)) {
          jversion = "1." + jversion;
        } else {
          throw new RuntimeException("Unparseable Java version '" + jversion + "', please consider setting '" + Configuration.WARP_JAVA_VERSION + "' explicitely in your configuration file.");
        }
      } else {
        // If version is of the form x-www or x+www, change it to
        // 1.x.0-www or 1.x.0+www

        if (jversion.contains("+")) {
          String extra = jversion.substring(jversion.indexOf("+"));
          jversion = jversion.substring(0, jversion.indexOf("+"));
          jversion = "1." + jversion + ".0" + extra;
        } else if (jversion.contains("-")) {
          String extra = jversion.substring(jversion.indexOf("-"));
          jversion = jversion.substring(0, jversion.indexOf("-"));
          jversion = "1." + jversion + ".0" + extra;
        } else if ("".equals(jversion.replaceAll("[0-9]+", ""))) {
          // simple number, e.g. '20'
          jversion = "1." + jversion + ".0";
        } else {
          throw new RuntimeException("Unparseable Java version '" + jversion + "', please consider setting '" + Configuration.WARP_JAVA_VERSION + "' explicitely in your configuration file.");
        }
      }
    }
    return jversion;
  }

  public static void main(String... args) {
    if (null != properties) {
      System.err.println("Properties already set");
      System.exit(-1);
    }

    if (StandardCharsets.UTF_8 != Charset.defaultCharset()) {
      throw new RuntimeException("Default encoding MUST be UTF-8 but it is " + Charset.defaultCharset() + ". Aborting.");
    }

    //
    // Copy file arguments up to '.'
    //

    List<String> lfiles = new ArrayList<String>();

    int keycount = 0;

    for (int i = 0; i < args.length; i++) {
      if (".".equals(args[i])) {
        keycount = args.length - i - 1;
        break;
      }
      lfiles.add(args[i]);
    }

    String[] files = lfiles.toArray(new String[lfiles.size()]);

    try {
      setProperties(false, files);

      for (int i = args.length - keycount; i < args.length; i++) {
        System.out.println("@CONF@ " + args[i] + "=" + getProperty(args[i]));
      }
    } catch (Throwable t) {
      System.err.println(ThrowableUtils.getErrorMessage(t));
      System.exit(-1);
    }
  }
}
