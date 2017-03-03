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

import com.google.common.base.Charsets;

/**
 * Class which manages file based Einstein macros from a directory
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
  private final String directory;
  
  /**
   * How often to check for changes
   */
  private final long delay;
  
  /**
   * Actual macros
   */
  private final static Map<String,Macro> macros = new HashMap<String,Macro>();
 
  /**
   * Fingerprints of the underlying files, will be checked to determine if macros should be updated or if we
   * can reuse the previous one
   */
  private static Map<String,Long> fingerprints = new HashMap<String, Long>();
  
  public WarpScriptMacroRepository(String directory, long delay) {
    this.directory = directory;
    this.delay = delay;
    
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
      
      List<File> files = getEinsteinFiles(this.directory);

      //
      // Loop over the files, creating the macros
      //
      
      Map<String, Macro> newmacros = new HashMap<String,Macro>();
      Map<String, Long> newfingerprints = new HashMap<String, Long>();
      
      byte[] buf = new byte[8192];
      
      boolean hasNew = false;
      
      for (File file: files) {
        
        String name = file.getAbsolutePath().substring(rootdir.length() + 1).replaceAll("\\.mc2$", "");
        
        //
        // Ignore '.mc2' files not in a subdir
        //
        
        if (!name.contains("/")) {
          continue;
        }
        
        //
        // Read content
        //
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(" ");
        
        try {
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
          
          if (fingerprints.containsKey(name) && hash == (long) fingerprints.get(name)) {
            newmacros.put(name, macros.get(name));
            newfingerprints.put(name, hash);
            continue;
          }
      
          hasNew = true;
          
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
                    
          // Make macro a secure one
          macro.setSecure(true);
          
          newmacros.put(name, macro);
          newfingerprints.put(name, hash);
        } catch(Exception e) {
          // Replace macro with a FAIL indicating the error message
          Macro macro = new Macro();
          macro.add("Error while loading macro '" + name + "': " + e.getMessage());
          macro.add(MSGFAIL_FUNC);
          newmacros.put(name, macro);
          hasNew = true;
        }
      }
      
      //
      // Replace the previous macros
      //
      
      if (hasNew) {
        synchronized(macros) {
          macros.clear();
          macros.putAll(newmacros);
          fingerprints = newfingerprints;
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
    synchronized(macros) {
      Macro macro = (Macro) macros.get(name);
      return macro;
    }    
  }
  
  public List<File> getEinsteinFiles(String rootdir) {
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
    
    long delay = DEFAULT_DELAY;
    
    String refresh = properties.getProperty(Configuration.REPOSITORY_REFRESH);

    if (null != refresh) {
      try {
        delay = Long.parseLong(refresh.toString());
      } catch (Exception e) {            
      }
    }

    new WarpScriptMacroRepository(dir, delay);
  }
}
