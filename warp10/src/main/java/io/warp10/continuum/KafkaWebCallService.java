//
//   Copyright 2019  SenX S.A.S.
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
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Preconditions;

public class KafkaWebCallService {  
  
  private static boolean initialized = false;
  
  private static KafkaProducer<byte[],byte[]> producer;
  
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

      ProducerRecord record = new ProducerRecord(topic, value);
      // We call get() so we have a synchronous producer behaviour
      producer.send(record).get();
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
    if (props.containsKey(Configuration.WEBCALL_KAFKA_AES)) {
      byte[] key = keystore.decodeKey(props.getProperty(Configuration.WEBCALL_KAFKA_AES));
      Preconditions.checkArgument((16 == key.length || 24 == key.length || 32 == key.length), Configuration.WEBCALL_KAFKA_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_KAFKA_WEBCALL, key);
      aesKey = key;
    }
    if (props.containsKey(Configuration.WEBCALL_KAFKA_MAC)) {
      byte[] key = keystore.decodeKey(props.getProperty(Configuration.WEBCALL_KAFKA_MAC));
      Preconditions.checkArgument((16 == key.length), Configuration.WEBCALL_KAFKA_MAC + " MUST be 128 bits long.");
      keystore.setKey(KeyStore.SIPHASH_KAFKA_WEBCALL, key);
      siphashKey = key;
    }    
  }
  
  private static void initialize() {
    Properties props = WarpConfig.getProperties();
    
    if (null == WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_CONSUMER_BOOTSTRAP_SERVERS)) {
      throw new RuntimeException(Configuration.WEBCALL_KAFKA_CONSUMER_BOOTSTRAP_SERVERS + " was not specified in the configuration.");
    }

    if (null == WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_PRODUCER_BOOTSTRAP_SERVERS)) {
      throw new RuntimeException(Configuration.WEBCALL_KAFKA_PRODUCER_BOOTSTRAP_SERVERS + " was not specified in the configuration.");
    }

    topic = WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_TOPIC);
    if (null == topic) {
      throw new RuntimeException(Configuration.WEBCALL_KAFKA_TOPIC + " was not specified in the configuration.");
    }

    Properties properties = new Properties();
    
    properties.putAll(Configuration.extractPrefixed(props, props.getProperty(Configuration.WEBCALL_KAFKA_PRODUCER_CONF_PREFIX)));

    // @see http://kafka.apache.org/documentation.html#producerconfigs
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_PRODUCER_BOOTSTRAP_SERVERS));
    
    if (null != props.getProperty(Configuration.WEBCALL_KAFKA_PRODUCER_CLIENTID)) {
      properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, WarpConfig.getProperty(Configuration.WEBCALL_KAFKA_PRODUCER_CLIENTID));
    }
    
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, io.warp10.continuum.KafkaPartitioner.class.getName());
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    
    producer = new KafkaProducer<byte[], byte[]>(properties);

    initialized = true;
  }  
}
