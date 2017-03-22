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

package io.warp10.script;

import io.warp10.WarpDist;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.INCLUDE;
import io.warp10.script.functions.MSGFAIL;
import io.warp10.sensision.Sensision;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;

/**
 * Class which manages file based WarpScript macros from a directory
 */
public class WarpScriptMacroRepository extends Thread {
  
  private static final MSGFAIL MSGFAIL_FUNC = new MSGFAIL("MSGFAIL");
  
  /**
   * Default refresh delay is 60 minutes
   */
  private static final long DEFAULT_DELAY = 3600000L;

  private static long[] SIP_KEYS = { 31232312312312L, 543534535435L };
  
  public static final String WARPSCRIPT_FILE_EXTENSION = ".mc2";
  
  /**
   * Directory where the '.mc2' files are
   */
  private static String directory;
  
  /**
   * How often to check for changes
   */
  private static long delay = DEFAULT_DELAY;
  
  /**
   * Counter to avoid recursive calls to loadMacro
   */
  private static ThreadLocal<AtomicInteger> loading = new ThreadLocal<AtomicInteger>() {
    @Override
    protected AtomicInteger initialValue() {
      return new AtomicInteger(0);
    }
  };
  
  /**
   * Should we enable loading macros on demand
   */
  private static boolean ondemand = false;
  
  /**
   * Actual macros
   */
  private final static Map<String,Macro> macros = new HashMap<String,Macro>();
 
  private WarpScriptMacroRepository() {
    this.setName("[Warp Macro Repository (" + directory + ")");
    this.setDaemon(true);
    this.start();
  }
    
  @Override
  public void run() {
    
    //
    // Wait until Continuum has initialized
    //
    
    while(!WarpDist.isInitialized()) {
      try { Thread.sleep(100L); } catch (InterruptedException ie) {}
    }
    
    //
    // Exit now if we do not run an 'egress' service
    //
    
    if (!WarpDist.isEgress()) {
      return;
    }
    
    while(true) {
            
      String rootdir = new File(this.directory).getAbsolutePath();
      
      //
      // Open directory
      //
      
      List<File> files = getWarpScriptFiles(this.directory);

      //
      // Loop over the files, creating the macros
      //
      
      Map<String, Macro> newmacros = new HashMap<String,Macro>();
            
      boolean hasNew = false;
      
      for (File file: files) {
        
        String name = file.getAbsolutePath().substring(rootdir.length() + 1).replaceAll("\\.mc2$", "");
        
        //
        // Ignore '.mc2' files not in a subdir
        //
        
        if (!name.contains("/")) {
          continue;
        }
        
        Macro macro = loadMacro(name, file);

        if (null != macro) {
          newmacros.put(name, macro);
          if (!macro.equals(macros.get(name))) {
            hasNew = true;
          }
        }        
      }
      
      //
      // Replace the previous macros
      //
      
      if (hasNew) {
        synchronized(macros) {
          macros.clear();
          macros.putAll(newmacros);
        }        
      }
      
      //
      // Update macro count
      //
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_EINSTEIN_REPOSITORY_MACROS, Sensision.EMPTY_LABELS, newmacros.size());
      
      //
      // Sleep a while
      //
      
      try {
        Thread.sleep(this.delay);
      } catch (InterruptedException ie) {        
      }
    }
  }
  
  public static Macro find(String name) throws WarpScriptException {
    Macro macro = null;
    synchronized(macros) {
      macro = (Macro) macros.get(name);
    }
    
    if (null == macro && ondemand) {
      macro = loadMacro(name, null);
      if (null != macro) {
        // Store the recently loaded macro in the map
        synchronized(macros) {
          macros.put(name, macro);
        }
      }
    }
      
    return macro;
  }
  
  public List<File> getWarpScriptFiles(String rootdir) {
    File root = new File(rootdir);
    
    // List to hold the directories
    final List<File> dirs = new ArrayList<File>();
    
    List<File> allfiles = new ArrayList<File>();

    if (!root.exists()) {
      return allfiles;
    }

    dirs.add(root);
    
    int idx = 0;    
    
    while(idx < dirs.size()) {
      File dir = dirs.get(idx++);
      
      File[] files = dir.listFiles(new FilenameFilter() {
        
        @Override
        public boolean accept(File dir, String name) {
          if (".".equals(name) || "..".equals(name)) {
            return false;
          }
          
          File f = new File(dir,name); 
          if (f.isDirectory()) {
            dirs.add(f);
            return false;
          } else if (name.endsWith(WARPSCRIPT_FILE_EXTENSION) || new File(dir, name).isDirectory()) {
            return true;
          }
          
          return false;
        }
      });
      
      if (null == files) {
        continue;
      }
      
      for (File file: files) {
        allfiles.add(file);
      }
    }
    
    return allfiles;
  }
  
  public static void init(Properties properties) {
    
    //
    // Extract root directory
    //
    
    String dir = properties.getProperty(Configuration.REPOSITORY_DIRECTORY);
    
    if (null == dir) {
      return;
    }
    
    //
    // Extract refresh interval
    //
    
    long refreshDelay = DEFAULT_DELAY;
    
    String refresh = properties.getProperty(Configuration.REPOSITORY_REFRESH);

    if (null != refresh) {
      try {
        refreshDelay = Long.parseLong(refresh.toString());
      } catch (Exception e) {            
      }
    }

    directory = dir;
    delay = refreshDelay;
    
    ondemand = "true".equals(properties.getProperty(Configuration.REPOSITORY_ONDEMAND));
    new WarpScriptMacroRepository();
  }
  
  /**
   * Load a macro stored in a file
   *
   * @param name of macro
   * @param file containing the macro
   * @return
   */
  private static Macro loadMacro(String name, File file) {
    
    if (null == name && null == file) {
      return null;
    }
    
    // Name should contain "/" as macros should reside under a subdirectory
    if (!name.contains("/")) {
      return null;
    }
    
    //
    // Read content
    //
    
    String rootdir = new File(directory).getAbsolutePath();
    
    if (null == file) {
      file = new File(rootdir, name + ".mc2");
      
      // Macros should reside in the configured root directory
      if (!file.getAbsolutePath().startsWith(rootdir)) {
        return null;
      }
    }

    if (null == name) {
      name = file.getAbsolutePath().substring(rootdir.length() + 1).replaceAll("\\.mc2$", "");
    }
    
    byte[] buf = new byte[8192];

    StringBuilder sb = new StringBuilder();
    
    sb.append(" ");
    
    try {
      
      if (loading.get().get() > 0) {
        throw new WarpScriptException("Invalid recursive macro loading.");
      }
      
      loading.get().addAndGet(1);
      
      FileInputStream in = new FileInputStream(file);
      ByteArrayOutputStream out = new ByteArrayOutputStream((int) file.length());
      
      while(true) {
        int len = in.read(buf);
        
        if (len < 0) {
          break;
        }
        
        out.write(buf, 0, len);
      }

      in.close();

      byte[] data = out.toByteArray();
      
      // Compute hash to check if the file changed or not
      
      long hash = SipHashInline.hash24_palindromic(SIP_KEYS[0], SIP_KEYS[1], data);

      Macro old = macros.get(name);
      
      // Re-use the same macro if its fingerprint did not change
      if (null != old && hash == old.getFingerprint()) {
        return old;
      }
            
      sb.append(new String(data, Charsets.UTF_8));
      
      sb.append("\n");
      
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
      stack.maxLimits();

      //
      // Add 'INCLUDE', 'enabled' will disable 'INCLUDE' after we've used it when loading 
      //

      // Root is the current subdir
      
      //
      // Create an instance of 'INCLUDE' for the current rootdir
      //
      
      // 'enabled' will allow us to disable the INCLUDE after loading the macro
      AtomicBoolean enabled = new AtomicBoolean(true);
      final INCLUDE include = new INCLUDE("INCLUDE", new File(rootdir, name.replaceAll("/.*", "")), enabled);

      stack.define("INCLUDE", new Macro() {
        public boolean isSecure() { return true; }
        public java.util.List<Object> statements() { return new ArrayList<Object>() {{ add(include); }}; }
        }
      );
      
      //
      // Execute the code
      //
      stack.execMulti(sb.toString());
      
      //
      // Disable 'INCLUDE'
      //
      
      enabled.set(false);
      
      //
      // Ensure the resulting stack is one level deep and has a macro on top
      //
      
      if (1 != stack.depth()) {
        throw new WarpScriptException("Stack depth was not 1 after the code execution.");
      }
      
      if (!(stack.peek() instanceof Macro)) {
        throw new WarpScriptException("No macro was found on top of the stack.");
      }
      
      //
      // Store resulting macro under 'name'
      //
      
      Macro macro = (Macro) stack.pop();
                
      macro.setFingerprint(hash);
      
      // Make macro a secure one
      macro.setSecure(true);
      
      return macro;
    } catch(Exception e) {
      // Replace macro with a FAIL indicating the error message
      Macro macro = new Macro();
      macro.add("Error while loading macro '" + name + "': " + e.getMessage());
      macro.add(MSGFAIL_FUNC);
      return macro;
    } finally {
      loading.get().addAndGet(-1);
    }
  }
}
