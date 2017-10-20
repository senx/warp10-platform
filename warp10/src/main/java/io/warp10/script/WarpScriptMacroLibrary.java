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

import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.INCLUDE;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import sun.net.www.protocol.file.FileURLConnection;

import com.google.common.base.Charsets;

/**
 * Macro library built by adding macros from various files, loaded from a root directory
 * or from the classpath
 * 
 * TODO(hbs): add support for secure script (the keystore is not initialized)
 */
public class WarpScriptMacroLibrary {
  private static final Map<String,Macro> macros = new HashMap<String, Macro>();
  
  public static void addJar(String path) throws WarpScriptException {
    addJar(path, null);
  }
  
  private static void addJar(String path, String resource) throws WarpScriptException {
    //
    // Exract basename of path
    //
    
    File f = new File(path);
    
    if (!f.exists() || !f.isFile()) {
      throw new WarpScriptException("File not found " + f.getAbsolutePath());
    }
    
    JarFile jar = null;
    
    try {
      String basename = f.getName();
      
      jar = new JarFile(f);
      
      Enumeration<JarEntry> entries = jar.entries();
      
      while(entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        
        if (entry.isDirectory()) {
          continue;
        }
        
        String name = entry.getName();
                
        if (!name.endsWith(WarpScriptMacroRepository.WARPSCRIPT_FILE_EXTENSION)) {
          continue;
        }
        
        if (null != resource && !resource.equals(name)) {
          continue;
        }
        
        name = name.substring(0, name.length() - WarpScriptMacroRepository.WARPSCRIPT_FILE_EXTENSION.length());
        
        InputStream in = jar.getInputStream(entry);

        Macro macro = loadMacro(jar, in);
        
        //
        // Store resulting macro under 'name'
        //
        
        // Make macro a secure one
        macro.setSecure(true);

        macros.put(name, macro);
      }
      
    } catch (IOException ioe) {
      throw new WarpScriptException("Encountered error while loading " + f.getAbsolutePath(), ioe);
    } finally {
      if (null != jar) { try { jar.close(); } catch (IOException ioe) {} }
    }    
  }
  
  public static Macro loadMacro(Object root, InputStream in) throws WarpScriptException {
    try {
      byte[] buf = new byte[8192];
      StringBuilder sb = new StringBuilder();

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      while(true) {
        int len = in.read(buf);
        
        if (len < 0) {
          break;
        }
        
        out.write(buf, 0, len);
      }

      in.close();

      byte[] data = out.toByteArray();

      sb.setLength(0);
      sb.append(" ");

      sb.append(new String(data, Charsets.UTF_8));
      
      sb.append("\n");
      
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
      stack.maxLimits();

      //
      // Add 'INCLUDE'
      //
      
      AtomicBoolean enabled = new AtomicBoolean(true);
      
      final INCLUDE include = root instanceof File ? new INCLUDE("INCLUDE", (File) root, enabled) : new INCLUDE("INCLUDE", (JarFile) root, enabled);
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
      // Disable INCLUDE
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
      
      Macro macro = (Macro) stack.pop();
      macro.setSecure(true);
      
      return macro;
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    } finally {
      try { in.close(); } catch (IOException ioe) {}
    }
  }
  
  public static Macro find(String name) throws WarpScriptException {    
    Macro macro = (Macro) macros.get(name);
    
    //
    // The macro is not (yet) known, we will attempt to load it from the
    // classpath
    //
    
    if (null == macro) {
      String rsc = name + WarpScriptMacroRepository.WARPSCRIPT_FILE_EXTENSION;
      URL url = WarpScriptMacroLibrary.class.getClassLoader().getResource(rsc);
      
      if (null != url) {
        try {
          URLConnection conn = url.openConnection();
          
          if (conn instanceof JarURLConnection) {
            //
            // This case is when the requested macro is in a jar
            //
            final JarURLConnection connection = (JarURLConnection) url.openConnection();
            final URL fileurl = connection.getJarFileURL();
            File f = new File(fileurl.toURI());
            addJar(f.getAbsolutePath(), rsc);
            macro = (Macro) macros.get(name);
          } else if (conn instanceof FileURLConnection) {
            //
            // This case is when the requested macro is in the classpath but not in a jar.
            // In this case we do not cache the parsed macro, allowing for dynamic modification.
            //
            String urlstr = url.toString();
            File root = new File(urlstr.substring(0, urlstr.length() - name.length()  - WarpScriptMacroRepository.WARPSCRIPT_FILE_EXTENSION.length()));
            macro = loadMacro(root, conn.getInputStream());
          }          
        } catch (URISyntaxException use) {
          throw new WarpScriptException("Error while loading '" + name + "'", use);
        } catch (IOException ioe) {
          throw new WarpScriptException("Error while loading '" + name + "'", ioe);
        }
      }
    }
    
    return macro;
  }
}
