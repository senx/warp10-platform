package org.apache.hadoop.hbase.client;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.ServerName;

public class ConnectionHelper {
  public static void clearMetaCache(Connection conn) {
    if (conn instanceof ConnectionManager.HConnectionImplementation) {
      ((ConnectionManager.HConnectionImplementation) conn).clearRegionCache();
    }
  }
  
  public static void publishPerServerMetrics(Connection conn) {
    if (!(conn instanceof ClusterConnection)) {
      return;
    }
    
    Map<ServerName,AtomicInteger> taskCounterPerServer = ((ClusterConnection) conn).getAsyncProcess().taskCounterPerServer;
    
    Map<String,String> labels = new HashMap<String, String>();
    
    for (Entry<ServerName,AtomicInteger> entry: taskCounterPerServer.entrySet()) {
      String serverName = entry.getKey().getServerName();
      int count = entry.getValue().get();
    
      labels.put(SensisionConstants.SENSISION_LABEL_SERVER, serverName);
      Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_HBASE_TASKS, labels, count);
    }
  }
}
