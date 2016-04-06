package org.apache.hadoop.hbase.client;

public class ConnectionHelper {
  public static void clearMetaCache(Connection conn) {
    if (conn instanceof ConnectionManager.HConnectionImplementation) {
      ((ConnectionManager.HConnectionImplementation) conn).clearRegionCache();
    }
  }
}
