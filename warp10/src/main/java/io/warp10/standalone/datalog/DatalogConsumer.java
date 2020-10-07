package io.warp10.standalone.datalog;

import io.warp10.crypto.KeyStore;

public interface DatalogConsumer {
  public void init(KeyStore ks, String name);
  public void success(String ref);
  public void failure(String ref);  
}
