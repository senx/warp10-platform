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

package io.warp10.standalone;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.KafkaDataMessageType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.primitives.Longs;

/**
 * Consumes Kafka topics for both data and metadata. Stores
 * the consumed data into the defined datastore and directory.
 */
public class StandaloneKafkaConsumer {
  
  private static final String STANDALONE_KAFKA_DATA_ZKCONNECT = "standalone.kafka.data.zkconnect";
  private static final String STANDALONE_KAFKA_DATA_TOPIC = "standalone.kafka.data.topic";
  private static final String STANDALONE_KAFKA_DATA_MAC = "standalone.kafka.data.mac";
  private static final String STANDALONE_KAFKA_DATA_AES = "standalone.kafka.data.aes";
  private static final String STANDALONE_KAFKA_DATA_GROUPID = "standalone.kafka.data.groupid";
  private static final String STANDALONE_KAFKA_DATA_COMMITPERIOD = "standalone.kafka.data.commitperiod";
  private static final String STANDALONE_KAFKA_DATA_NTHREADS = "standalone.kafka.data.nthreads";

  private static final String STANDALONE_KAFKA_METADATA_ZKCONNECT = "standalone.kafka.metadata.zkconnect";
  private static final String STANDALONE_KAFKA_METADATA_TOPIC = "standalone.kafka.metadata.topic";
  private static final String STANDALONE_KAFKA_METADATA_MAC = "standalone.kafka.metadata.mac";
  private static final String STANDALONE_KAFKA_METADATA_AES = "standalone.kafka.metadata.aes";
  private static final String STANDALONE_KAFKA_METADATA_GROUPID = "standalone.kafka.metadata.groupid";
  private static final String STANDALONE_KAFKA_METADATA_COMMITPERIOD = "standalone.kafka.metadata.commitperiod";
  private static final String STANDALONE_KAFKA_METADATA_NTHREADS = "standalone.kafka.metadata.nthreads";

  private static interface ConsumerFactory {
    public Runnable getConsumer(KafkaConsumerWrapper wrapper, KafkaStream<byte[], byte[]> stream);
  }
  
  private static final class DataConsumer implements Runnable {

    private final KeyStore keystore;
    private final StoreClient store;
    private final StandaloneDirectoryClient directory;
    private final KafkaConsumerWrapper wrapper;
    private final KafkaStream<byte[],byte[]> stream;
        
    public DataConsumer(KeyStore keystore, StoreClient store, StandaloneDirectoryClient directoryClient, KafkaConsumerWrapper wrapper, KafkaStream<byte[], byte[]> stream) {
      this.keystore = keystore;
      this.store = store;
      this.directory = directoryClient;
      this.wrapper = wrapper;
      this.stream = stream;
    }
    
    @Override
    public void run() {
      
      Thread.currentThread().setName("[Kafka Consumer]");

      long count = 0L;
      
      try {
        ConsumerIterator<byte[],byte[]> iter = this.stream.iterator();

        
        byte[] siphashKey = keystore.decodeKey(wrapper.mac);
        byte[] aesKey = keystore.decodeKey(wrapper.aes);
        
        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        // TODO(hbs): allow setting of writeBufferSize

        while (iter.hasNext()) {
          //
          // Since the cal to 'next' may block, we need to first
          // check that there is a message available, otherwise we
          // will miss the synchronization point with the other
          // threads.
          //
          
          boolean nonEmpty = iter.nonEmpty();
          
          if (nonEmpty) {
            count++;
            MessageAndMetadata<byte[], byte[]> msg = iter.next();
            
            byte[] data = msg.message();

            // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_STANDALONE_KAFKA__DATA_COUNT, Sensision.EMPTY_LABELS, 1);
            // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_STANDALONE_KAFKA_DATA_BYTES, Sensision.EMPTY_LABELS, data.length);
            
            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }
            
            // Skip data whose MAC was not verified successfully
            if (null == data) {
              // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_STANDALONE_KAFKA_DATA_FAILEDMACS, Sensision.EMPTY_LABELS, 1);
              continue;
            }
            
            // Unwrap data if need be
            if (null != aesKey) {
              data = CryptoUtils.unwrap(aesKey, data);
            }
            
            // Skip data that was not unwrapped successfuly
            if (null == data) {
              // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_STANDALONE_KAFKA_DATA_FAILEDDECRYPTS, Sensision.EMPTY_LABELS, 1);
              // TODO(hbs): increment Sensision metric
              continue;
            }
            
            //
            // Extract KafkaDataMessage
            //
            
            KafkaDataMessage tmsg = new KafkaDataMessage();
            deserializer.deserialize(tmsg, data);
            
            switch(tmsg.getType()) {
              case STORE:
                handleStore(tmsg);
                break;
              case ARCHIVE:
                break;
              case DELETE:
                break;
            }          
          } else {
            // Sleep a tiny while
            try {
              Thread.sleep(2L);
            } catch (InterruptedException ie) {             
            }
          }          
        }        
      } catch (Throwable t) {
        // FIXME(hbs): log something/update Sensision metrics
        t.printStackTrace(System.err);
      } finally {
        // Set abort to true in case we exit the 'run' method
        wrapper.abort.set(true);
      }
    }
    
    private void handleStore(KafkaDataMessage msg) throws IOException {
      
      if (KafkaDataMessageType.STORE != msg.getType()) {
        return;
      }
      // Skip if there are no data to decode
      if (null == msg.getData() || 0 == msg.getData().length) {
        return;
      }
      
      //
      // Create a GTSDecoder with the given readings (@see Ingress.java for the packed format)
      //
      
      GTSDecoder decoder = new GTSDecoder(0L, ByteBuffer.wrap(msg.getData()));
      
      // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_STANDALONE_KAFKA_GTSDECODERS,  Sensision.EMPTY_LABELS, 1);
      
      long datapoints = 0L;
      
      GTSEncoder encoder = new GTSEncoder(0L);
      
      // Set classId/labelsId in the encoder as it does not have MetaData
      encoder.setClassId(msg.getClassId());
      encoder.setLabelsId(msg.getLabelsId());
      
      while(decoder.next()) {
        encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getValue());                            
        datapoints++;
      }
      
      store.store(encoder);
      //Sensision.update(SensisionConstants.SENSISION_CLASS_STANDALONE_HBASE_PUTS, Sensision.EMPTY_LABELS, datapoints);
    }
    
    private void handleDelete(KafkaDataMessage msg) throws IOException {
      if (KafkaDataMessageType.DELETE != msg.getType()) {
        return;
      }

      Metadata metadata = new Metadata();
      metadata.setClassId(msg.getClassId());
      metadata.setLabelsId(msg.getLabelsId());
      
      //
      // Remove data
      //
      
      this.store.delete(null, metadata, msg.getDeletionStartTimestamp(), msg.getDeletionEndTimestamp());      
    }
  }
  
  private static final class MetadataConsumer implements Runnable {
    private final KeyStore keystore;
    private final StoreClient storeClient;
    private final StandaloneDirectoryClient directoryClient;
    private final KafkaConsumerWrapper wrapper;
    private final KafkaStream<byte[],byte[]> stream;
        
    public MetadataConsumer(final KeyStore keystore, final StoreClient storeClient, final StandaloneDirectoryClient directoryClient, KafkaConsumerWrapper wrapper, KafkaStream<byte[], byte[]> stream) {      
      this.keystore = keystore;
      this.storeClient = storeClient;
      this.directoryClient = directoryClient;
      this.wrapper = wrapper;
      this.stream = stream;
    }
    
    @Override
    public void run() {
      long count = 0L;
      
      try {
        ConsumerIterator<byte[],byte[]> iter = this.stream.iterator();

        byte[] siphashKey = keystore.decodeKey(wrapper.mac);
        byte[] aesKey = keystore.decodeKey(wrapper.aes);
            
        
        // TODO(hbs): allow setting of writeBufferSize

        while (iter.hasNext()) {
          //
          // Since the cal to 'next' may block, we need to first
          // check that there is a message available, otherwise we
          // will miss the synchronization point with the other
          // threads.
          //
          
          boolean nonEmpty = iter.nonEmpty();
          
          if (nonEmpty) {
            count++;
            MessageAndMetadata<byte[], byte[]> msg = iter.next();
            
            byte[] data = msg.message();
            
            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }
            
            // Skip data whose MAC was not verified successfully
            if (null == data) {
              // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDMACS, Sensision.EMPTY_LABELS, 1);
              continue;
            }
            
            // Unwrap data if need be
            if (null != aesKey) {
              data = CryptoUtils.unwrap(aesKey, data);
            }
            
            // Skip data that was not unwrapped successfuly
            if (null == data) {
              // TODO(hbs): Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDDECRYPTS, Sensision.EMPTY_LABELS, 1);
              continue;
            }
            
            //
            // TODO(hbs): We could check that metadata class/labels Id match those of the key, but
            // since it was wrapped/authenticated, we suppose it's ok.
            //
                        
            byte[] labelsBytes = Arrays.copyOfRange(data, 8, 16);
            long labelsId = Longs.fromByteArray(labelsBytes);

            byte[] metadataBytes = Arrays.copyOfRange(data, 16, data.length);
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            Metadata metadata = new Metadata();
            deserializer.deserialize(metadata, metadataBytes);
            
            //
            // Force Attributes
            //
            
            if (!metadata.isSetAttributes()) {
              metadata.setAttributes(new HashMap<String,String>());
            }
            
            //
            // Check the source of the metadata
            //

            //
            // If Metadata is from Delete, remove it from the StandaloneDirectoryClient
            //
            
            if (Configuration.INGRESS_METADATA_DELETE_SOURCE.equals(metadata.getSource())) {
              directoryClient.unregister(metadata);
              storeClient.delete(null, metadata, Long.MIN_VALUE, Long.MAX_VALUE);
              continue;
            }                        

            directoryClient.register(metadata);
          } else {
            // Sleep a tiny while
            try {
              Thread.sleep(2L);
            } catch (InterruptedException ie) {             
            }
          }          
        }        
      } catch (Throwable t) {
        t.printStackTrace(System.err);
      } finally {
        // Set abort to true in case we exit the 'run' method
        wrapper.abort.set(true);
      }
    }
    
  }
  
  private static final class KafkaConsumerWrapper extends Thread {
    
    private final String zkconnect;
    private final String topic;
    private final String mac;
    private final String aes;
    private final String groupid;
    private final long commitPeriod;
    private final int nthreads;
    private final ConsumerFactory factory;
    
    /**
     * Flag indicating an abort
     */
    private final AtomicBoolean abort = new AtomicBoolean(false);
    
    public KafkaConsumerWrapper(String zkconnect, String topic, String mac, String aes, String groupid, long commitPeriod, int nthreads, ConsumerFactory factory) {
      
      this.zkconnect = zkconnect;
      this.topic = topic;
      this.mac = mac;
      this.aes = aes;
      this.groupid = groupid;
      this.commitPeriod = commitPeriod;
      this.nthreads = nthreads;
      this.factory = factory;
      
      this.setDaemon(true);
      this.start();
    }
    
    @Override
    public void run() {
      
      ExecutorService executor = null;
      ConsumerConnector connector = null;
      
      while(true) {
        try {
          //
          // Enter an endless loop which will spawn 'nthreads' threads
          // each time the Kafka consumer is shut down (which will happen if an error
          // happens while talking to HBase, to get a chance to re-read data from the
          // previous snapshot).
          //
          
          Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
            
          topicCountMap.put(topic, nthreads);
            
          Properties props = new Properties();
          props.setProperty("zookeeper.connect", zkconnect);
          props.setProperty("group.id", groupid);
          props.setProperty("auto.commit.enable", "false");    
          
          ConsumerConfig config = new ConsumerConfig(props);
          connector = Consumer.createJavaConsumerConnector(config);
          
          Map<String,List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
          
          List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
          
          executor = Executors.newFixedThreadPool(nthreads);
          
          //
          // now create runnables which will consume messages
          //
          
          for (final KafkaStream<byte[],byte[]> stream : streams) {
            executor.submit(factory.getConsumer(this, stream));
          }      

          long lastCommit = System.currentTimeMillis();
          
          while(!abort.get()) {
            try {
              long sleepUntil = lastCommit + commitPeriod;
              long delta = sleepUntil - System.currentTimeMillis();
              
              if (delta > 0) {
                Thread.sleep(delta);
              }
            } catch (InterruptedException ie) {
              continue;
            }

            // Commit offsets
            connector.commitOffsets();
            lastCommit = System.currentTimeMillis();
            
            //Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_KAFKA_DATA_COMMITS, Sensision.EMPTY_LABELS, 1);
          }
        } catch (Throwable t) {
          t.printStackTrace(System.err);
        } finally {
          //
          // We exited the loop, this means one of the threads triggered an abort,
          // we will shut down the executor and shut down the connector to start over.
          //
      
          if (null != executor) {
            try {
              executor.shutdownNow();
            } catch (Exception e) {                
            }
          }
          if (null != connector) {
            try {
              connector.shutdown();
            } catch (Exception e) {
              
            }
          }
          
          //Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_ABORTS, Sensision.EMPTY_LABELS, 1);
          
          abort.set(false);

          try { Thread.sleep(1000L); } catch (InterruptedException ie) {}
        }
      }      
    }
  }
  
  public StandaloneKafkaConsumer(final KeyStore keystore, final StandaloneMemoryStore storeClient, final StandaloneDirectoryClient directoryClient) {    
    final Properties properties = WarpConfig.getProperties();
    
    //
    // Create the metadata consuming Thread
    //

    KafkaConsumerWrapper metadataWrapper = new KafkaConsumerWrapper(
        properties.getProperty(STANDALONE_KAFKA_METADATA_ZKCONNECT),
        properties.getProperty(STANDALONE_KAFKA_METADATA_TOPIC),
        properties.getProperty(STANDALONE_KAFKA_METADATA_MAC),
        properties.getProperty(STANDALONE_KAFKA_METADATA_AES),
        properties.getProperty(STANDALONE_KAFKA_METADATA_GROUPID),
        Long.valueOf(properties.getProperty(STANDALONE_KAFKA_METADATA_COMMITPERIOD)),
        Integer.valueOf(properties.getProperty(STANDALONE_KAFKA_METADATA_NTHREADS)),
        new ConsumerFactory() {          
          @Override
          public Runnable getConsumer(KafkaConsumerWrapper wrapper, KafkaStream<byte[], byte[]> stream) {
            return new MetadataConsumer(keystore, storeClient, directoryClient, wrapper, stream);
          }
        });
    metadataWrapper.setName("[Standalone Kafka Metadata Consumer Wrapper]");

    //
    // Create the data consuming Thread
    //
    
    KafkaConsumerWrapper dataWrapper = new KafkaConsumerWrapper(
        properties.getProperty(STANDALONE_KAFKA_DATA_ZKCONNECT),
        properties.getProperty(STANDALONE_KAFKA_DATA_TOPIC),
        properties.getProperty(STANDALONE_KAFKA_DATA_MAC),
        properties.getProperty(STANDALONE_KAFKA_DATA_AES),
        properties.getProperty(STANDALONE_KAFKA_DATA_GROUPID),
        Long.valueOf(properties.getProperty(STANDALONE_KAFKA_DATA_COMMITPERIOD)),
        Integer.valueOf(properties.getProperty(STANDALONE_KAFKA_DATA_NTHREADS)),
        new ConsumerFactory() {          
          @Override
          public Runnable getConsumer(KafkaConsumerWrapper wrapper, KafkaStream<byte[], byte[]> stream) {
            return new DataConsumer(keystore, storeClient, directoryClient, wrapper, stream);
          }
        });
    dataWrapper.setName("[Standalone Kafka Data Consumer Wrapper]");
  }
}
