package io.warp10.standalone;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

public class WarpInit {
  public static void main(String[] args) throws IOException {
    String path = args[0];
    
    Options options = new Options();
    options.createIfMissing(true);
    options.verifyChecksums(true);
    options.paranoidChecks(true);

    DB db = null;

    try {
      db = JniDBFactory.factory.open(new File(path), options);
    } catch (UnsatisfiedLinkError ule) {
      db = Iq80DBFactory.factory.open(new File(path), options);
    }      
    
    db.close();
  }
}
