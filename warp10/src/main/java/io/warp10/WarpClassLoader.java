package io.warp10;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarpClassLoader extends ClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(WarpClassLoader.class);
  
  private final String jarpath;
  
  public WarpClassLoader(String jarpath, ClassLoader parent) {
    super(parent);
    this.jarpath = jarpath;
    registerAsParallelCapable();
  }
  
  
  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return loadClass(name, false);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      Class c = findClass(name);
      super.resolveClass(c);
      return c;
    } catch (ClassNotFoundException cnfe) {
      return super.loadClass(name, resolve);
    } finally {
    }
  }
  
  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    String clsFile = name.replace('.', '/') + ".class";
    
    JarFile jf = null;
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream in = null;
    
    try {
    
      jf = new JarFile(this.jarpath);
      ZipEntry entry = jf.getEntry(clsFile);
      
      if (null == entry) {
        throw new ClassNotFoundException();
      }
      
      in = jf.getInputStream(entry);
      
      if (null == in) {
        return null;
      }
      
      byte[] buffer = new byte[1024];

      while (true) {
        int len = in.read(buffer, 0, buffer.length);
        
        if (-1 == len) {
          break;
        }
        
        out.write(buffer, 0, len);
      }
    } catch (IOException ioe) {
      throw new ClassNotFoundException("",ioe);
    } finally {
      if (null != in) {
        try { in.close(); } catch (IOException ioe) {}
      }
      if (null != jf) {        
        try { jf.close(); } catch (IOException ioe) {}
      }
    }

    byte[] data = out.toByteArray();

    //
    // Return class
    //
    
    try {
      Class c = defineClass(name, data, 0, data.length);
    
      return c;
    } catch (Exception e) {
      LOG.error("Error calling defineClass(" + name + ")", e);
      throw new ClassNotFoundException("Error calling defineClass(" + name + ")", e);
    }
  }
}
