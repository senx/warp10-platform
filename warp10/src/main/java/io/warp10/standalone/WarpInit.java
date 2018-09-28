package io.warp10.standalone;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import io.warp10.continuum.Configuration;

public class WarpInit {
  public static void main(String[] args) throws IOException {
    String path = args[0];
    
    Options options = new Options();
    options.createIfMissing(true);
    options.verifyChecksums(true);
    options.paranoidChecks(true);

    DB db = null;

    boolean nativedisabled = "true".equals(System.getProperty(Configuration.LEVELDB_NATIVE_DISABLE));
    boolean javadisabled = "true".equals(System.getProperty(Configuration.LEVELDB_JAVA_DISABLE));
    
    try {
      if (!nativedisabled) {
        db = JniDBFactory.factory.open(new File(path), options);
      } else {
        throw new UnsatisfiedLinkError("Native LevelDB implementation disabled.");
      }
    } catch (UnsatisfiedLinkError ule) {
      ule.printStackTrace();
      if (!javadisabled) {
        db = Iq80DBFactory.factory.open(new File(path), options);
      } else {
        throw new RuntimeException("No usable LevelDB implementation, aborting.");
      }
    }      
    
    db.close();
  }
}
