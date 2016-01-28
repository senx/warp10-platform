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

package io.warp10.continuum;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class used to keep track of Kafka offsets
 */
public class KafkaOffsetCounters {
  private static final int GROWBY = 16;
  
  private final String topic;
  private final String groupid;
  
  private AtomicLong[] counters = new AtomicLong[128];
  
  private final long ttl;
  
  public KafkaOffsetCounters(String topic, String groupid, long ttl) {
    this.topic = topic;
    this.groupid = groupid;
    this.ttl = ttl;
  }
  
  /**
   * Remove per partition counters and associated Sensision metrics
   */
  public synchronized void reset() {
    Map<String,String> labels = new HashMap<String,String>();    
    labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
    labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
    
    for (int i = 0; i < this.counters.length; i++) {
      if (null == this.counters[i]) {
        continue;
      }
      
      labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(i));
      Sensision.clear(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET, labels);
      
      this.counters[i] = null;
    }
  }
  
  public synchronized void count(int partition, long offset) {
    
    AtomicLong counter = null;
    
    if (partition >= this.counters.length) {
      this.counters = Arrays.copyOf(this.counters, this.counters.length + GROWBY);
    }
    counter = counters[partition];
    if (null == counter) {
      counter = new AtomicLong(0L);
      counters[partition] = counter;
    }
    
    counter.set(offset);
  }
  
  public synchronized void sensisionPublish() {
    Map<String,String> labels = new HashMap<String,String>();    
    labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
    labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
    
    for (int i = 0; i < this.counters.length; i++) {
      if (null == this.counters[i]) {
        continue;
      }
      
      labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(i));
      Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET, labels, this.counters[i].get(), ttl);
      // Reset counter so we do not continue publishing an old value
      this.counters[i] = null;
    }
  }
}
