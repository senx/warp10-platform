package io.warp10.standalone;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

public class WarpRepair {
  public static void main(String[] args) throws IOException {
    String path = args[0];
    
    Options options = new Options();
    options.createIfMissing(false);
    options.maxOpenFiles(200);
    options.verifyChecksums(true);
    options.paranoidChecks(true);
    try {
      JniDBFactory.factory.repair(new File(path), options);
    } catch (UnsatisfiedLinkError ule) {
      Iq80DBFactory.factory.repair(new File(path), options);
    }      
  }
}
