//
//   Copyright 2018-2020  SenX S.A.S.
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

import io.warp10.WarpConfig;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.script.thrift.data.WebCallRequest;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

public class KafkaWebCallService {
  
  private static boolean initialized = false;
  
  private static Producer<byte[],byte[]> producer;
  
  private static String topic;
  
  private static byte[] aesKey = null;
      
  private static byte[] siphashKey = null;
  
  public static synchronized boolean offer(WebCallRequest request) {    
    try {
      //
      // Initialize service if not done yet
      //
      
      if (!initialized) {
        initialize();
      }
    
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      
      byte[] value = serializer.serialize(request);
      
      //
      // Wrap data if AES key is defined
      //
      
      if (null != aesKey) {
        value = CryptoUtils.wrap(aesKey, value);               
      }
      
      //
      // Compute MAC if the SipHash key is defined
      //
      
      if (null != siphashKey) {
        value = CryptoUtils.addMAC(siphashKey, value);
      }

      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(topic, value);
      
      producer.send(message);
      
      return true;
    } catch (Exception e) {
      return false;
    }
  }
  
  /**
   * Extract the required keys if they exist
   * 
   * @param keystore
   */
  public static void initKeys(KeyStore keystore, Properties props) {
    aesKey = KeyStore.checkAndSetKey(keystore, KeyStore.AES_KAFKA_WEBCALL, props, Configuration.WEBCALL_KAFKA_AES, 128, 192, 256);
    siphashKey = KeyStore.checkAndSetKey(keystore, KeyStore.SIPHASH_KAFKA_WEBCALL, props, Configuration.WEBCALL_KAFKA_MAC, 128);

    // the keystore.forget() call is the responsability of the caller.
  }
  
  private static void initialize() {
    if (null == WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_ZKCONNECT)) {
      throw new RuntimeException(Configuration.WEBCALL_KAFKA_ZKCONNECT + " was not specified in the configuration.");
    }

    String brokerListProp = WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_BROKERLIST);
    if (null == brokerListProp) {
      throw new RuntimeException(Configuration.WEBCALL_KAFKA_BROKERLIST + " was not specified in the configuration.");
    }

    topic = WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_TOPIC);
    if (null == topic) {
      throw new RuntimeException(Configuration.WEBCALL_KAFKA_TOPIC + " was not specified in the configuration.");
    }

    Properties properties = new Properties();
    // @see <a href="http://kafka.apache.org/documentation.html#producerconfigs">http://kafka.apache.org/documentation.html#producerconfigs</a>
    properties.setProperty("metadata.broker.list", brokerListProp);

    String producerClientIdProp = WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_PRODUCER_CLIENTID);
    if (null != producerClientIdProp) {
      properties.setProperty("client.id", producerClientIdProp);
    }
    
    properties.setProperty("request.required.acks", "-1");
    properties.setProperty("producer.type","sync");
    properties.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    
    ProducerConfig config = new ProducerConfig(properties);
    producer = new Producer<byte[], byte[]>(config);

    initialized = true;
  }  
}
