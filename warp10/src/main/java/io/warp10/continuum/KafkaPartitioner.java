//
//   Copyright 2019-2020  SenX S.A.S.
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

import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import io.warp10.crypto.SipHashInline;

public class KafkaPartitioner implements Partitioner {
  
  private static final long SIPHASH_KEY_MSB = 0x7CE95E3A16DB4FA3L;
  private static final long SIPHASH_KEY_LSB = 0x95037E0C0DB5B059L;
  
  private Random random = new Random();
    
  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    int numPartitions = cluster.partitionCountForTopic(topic);

    if (null == key || 0 == ((byte[]) keyBytes).length) {
      return random.nextInt(numPartitions);
    } else {
      byte[] bytes = (byte[]) key;
      
      long k = SipHashInline.hash24(SIPHASH_KEY_MSB, SIPHASH_KEY_LSB, bytes, 0, bytes.length);
            
      return (int) ((k & 0x7FFFFFFFL) % numPartitions);
    }
  }
  
  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public void close() {}
}
