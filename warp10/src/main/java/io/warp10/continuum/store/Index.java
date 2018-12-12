//
//   Copyright 2018  SenX S.A.S.
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

package io.warp10.continuum.store;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.index.thrift.data.IndexComponent;
import io.warp10.continuum.index.thrift.data.IndexComponentType;
import io.warp10.continuum.index.thrift.data.IndexSpec;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.DirectoryRowFilter;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;

import com.geoxp.GeoXPLib;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

/**
 * This class implements the indexing process.
 * 
 * At init, an Index instance reads index descriptions from HBase and retains the indices
 * whose indexId modulo a parameterized modulus matches a given remainder (this allows for
 * spreading responsibility among N Index instances).
 * 
 * Once the index descriptions are read, the GTS metadata are read from HBase and associated with
 * the indices they are concerned by.
 * 
 * Once initialized, an Index instance will spawn threads which will consume data from the
 * 'data' Kafka topic. If the incoming data belongs to a GTS which should be indexed, the index keys are
 * generated and the data written to HBase.
 * 
 * The Index instance also listens to the 'meta' topic to get notified of new metadata. Any new metadata coming
 * in will be associated with its indices.
 * 
 */
public class Index {
  /**
   * Key to use for encrypting data in HBase (128/192/256 bits in hex or OSS reference) 
   */
  private static final String INDEX_HBASE_DATA_AES = "index.hbase.data.aes";
  
  /**
   * Columns family under which data should be stored
   */
  private static final String INDEX_HBASE_DATA_COLFAM = "index.hbase.data.colfam";
  
  /**
   * HBase table where data should be stored
   */
  private static final String INDEX_HBASE_DATA_TABLE = "index.hbase.data.table";
  
  /**
   * ZooKeeper Quorum for locating HBase
   */
  private static final String INDEX_HBASE_DATA_ZKCONNECT = "index.hbase.data.zkconnect";
  
  /**
   * Parent znode under which HBase znodes will be created
   */
  private static final String INDEX_HBASE_DATA_ZNODE = "index.hbase.data.znode";

  /**
   * Key to use for encrypting index descriptions in HBase (128/192/256 bits in hex or OSS reference) 
   */
  private static final String INDEX_HBASE_INDEX_AES = "index.hbase.index.aes";

  /**
   * Columns family under which metadata should be stored
   */
  private static final String INDEX_HBASE_INDEX_COLFAM = "index.hbase.index.colfam";

  
  
  /**
   * HBase table where metadata should be stored
   */
  private static final String INDEX_HBASE_INDEX_TABLE = "index.hbase.index.table";
  
  /**
   * ZooKeeper Quorum for locating HBase
   */
  private static final String INDEX_HBASE_INDEX_ZKCONNECT = "index.hbase.index.zkconnect";
  
  /**
   * Parent znode under which HBase znodes will be created
   */
  private static final String INDEX_HBASE_INDEX_ZNODE = "index.hbase.index.znode";
  
  /**
   * Key to use for encrypting metadata in HBase (128/192/256 bits in hex or OSS reference) 
   */
  private static final String INDEX_HBASE_METADATA_AES = "index.hbase.metadata.aes";
  
  /**
   * Columns family under which metadata should be stored
   */
  private static final String INDEX_HBASE_METADATA_COLFAM = "index.hbase.metadata.colfam";

  /**
   * HBase table where metadata should be stored
   */
  private static final String INDEX_HBASE_METADATA_TABLE = "index.hbase.metadata.table";

  /**
   * ZooKeeper Quorum for locating HBase
   */
  private static final String INDEX_HBASE_METADATA_ZKCONNECT = "index.hbase.metadata.zkconnect";

  /**
   * Parent znode under which HBase znodes will be created
   */
  private static final String INDEX_HBASE_METADATA_ZNODE = "index.hbase.metadata.znode";


  
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  private static final String INDEX_KAFKA_DATA_AES = "index.kafka.data.aes";
  
  /**
   * Kafka broker list for the 'data' topic
   */
  private static final String INDEX_KAFKA_DATA_BROKERLIST = "index.kafka.data.brokerlist";
  
  /**
   * Delay between synchronization for offset commit
   */
  private static final String INDEX_KAFKA_DATA_COMMITPERIOD = "index.kafka.data.commitperiod";
  
  /**
   * Kafka group id with which to consume the data topic
   */
  private static final String INDEX_KAFKA_DATA_GROUPID = "index.kafka.data.groupid";

  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  private static final String INDEX_KAFKA_DATA_MAC = "index.kafka.data.mac";


  
  /**
   * Number of threads to run for ingesting data from Kafka
   */
  private static final String INDEX_KAFKA_DATA_NTHREADS = "index.kafka.data.nthreads";
  
  /**
   * Actual 'data' topic
   */
  private static final String INDEX_KAFKA_DATA_TOPIC = "index.kafka.data.topic";
  
  /**
   * Zookeeper ZK connect string for Kafka ('data' topic)
   */  
  private static final String INDEX_KAFKA_DATA_ZKCONNECT = "index.kafka.data.zkconnect";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  private static final String INDEX_KAFKA_METADATA_AES = "index.kafka.metadata.aes";

  /**
   * Kafka broker list for the 'metadata' topic
   */
  private static final String INDEX_KAFKA_METADATA_BROKERLIST = "index.kafka.metadata.brokerlist";

  /**
   * Delay between synchronization for offset commit
   */
  private static final String INDEX_KAFKA_METADATA_COMMITPERIOD = "index.kafka.metadata.commitperiod";
  
  /**
   * Kafka group id with which to consume the metadata topic
   */
  private static final String INDEX_KAFKA_METADATA_GROUPID = "index.kafka.metadata.groupid";
  
  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  private static final String INDEX_KAFKA_METADATA_MAC = "index.kafka.metadata.mac";
  
  /**
   * Actual 'metadata' topic
   */
  private static final String INDEX_KAFKA_METADATA_TOPIC = "index.kafka.metadata.topic";

  /**
   * Zookeeper ZK connect string for Kafka ('metadata' topic)
   */  
  private static final String INDEX_KAFKA_METADATA_ZKCONNECT = "index.kafka.metadata.zkconnect";

  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  private static final String INDEX_KAFKA_INDEX_AES = "index.kafka.index.aes";

  /**
   * Kafka broker list for the 'index' topic
   */
  private static final String INDEX_KAFKA_INDEX_BROKERLIST = "index.kafka.index.brokerlist";

  /**
   * Delay between synchronization for offset commit
   */
  private static final String INDEX_KAFKA_INDEX_COMMITPERIOD = "index.kafka.index.commitperiod";
  
  /**
   * Kafka group id with which to consume the metadata topic
   */
  private static final String INDEX_KAFKA_INDEX_GROUPID = "index.kafka.index.groupid";
  
  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  private static final String INDEX_KAFKA_INDEX_MAC = "index.kafka.index.mac";
  
  /**
   * Actual 'index' topic
   */
  private static final String INDEX_KAFKA_INDEX_TOPIC = "index.kafka.index.topic";

  /**
   * Zookeeper ZK connect string for Kafka ('index' topic)
   */  
  private static final String INDEX_KAFKA_INDEX_ZKCONNECT = "index.kafka.index.zkconnect";


  /**
   * Partition of metadatas we focus on, format is MODULUS:REMAINDER
   */
  private static final String INDEX_PARTITION = "index.partition";
  
  /**
   * Port on which the DirectoryService will listen
   */
  private static final String INDEX_PORT = "index.port";
  
  /**
   * Number of threads to run for serving directory requests
   */
  private static final String INDEX_SERVICE_NTHREADS = "index.service.nthreads";

  /**
   * ZooKeeper server list for registering
   */
  private static final String INDEX_ZK_QUORUM = "index.zk.quorum";
  
  /**
   * ZooKeeper znode under which to register
   */
  private static final String INDEX_ZK_ZNODE = "index.zk.znode";

  private static final byte[] HBASE_INDEX_KEY_PREFIX = "I".getBytes(Charsets.UTF_8);
  
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    INDEX_ZK_QUORUM,
    INDEX_ZK_ZNODE,
    INDEX_SERVICE_NTHREADS,
    
    INDEX_PARTITION,
    INDEX_PORT,
    
    INDEX_KAFKA_METADATA_ZKCONNECT,
    INDEX_KAFKA_METADATA_BROKERLIST,
    INDEX_KAFKA_METADATA_TOPIC,
    INDEX_KAFKA_METADATA_GROUPID,
    INDEX_KAFKA_METADATA_COMMITPERIOD,
    //INDEX_KAFKA_METADATA_AES,
    //INDEX_KAFKA_METADATA_MAC,

    INDEX_KAFKA_DATA_ZKCONNECT,
    INDEX_KAFKA_DATA_NTHREADS,
    INDEX_KAFKA_DATA_BROKERLIST,
    INDEX_KAFKA_DATA_TOPIC,
    INDEX_KAFKA_DATA_GROUPID,
    INDEX_KAFKA_DATA_COMMITPERIOD,
    //INDEX_KAFKA_DATA_AES,
    //INDEX_KAFKA_DATA_MAC,

    INDEX_KAFKA_INDEX_ZKCONNECT,
    INDEX_KAFKA_INDEX_BROKERLIST,
    INDEX_KAFKA_INDEX_TOPIC,
    INDEX_KAFKA_INDEX_GROUPID,
    INDEX_KAFKA_INDEX_COMMITPERIOD,
    //INDEX_KAFKA_INDEX_AES,
    //INDEX_KAFKA_INDEX_MAC,

    /*
    INDEX_KAFKA_INDEX_ZKCONNECT,
    INDEX_KAFKA_INDEX_BROKERLIST,
    INDEX_KAFKA_INDEX_TOPIC,
    INDEX_KAFKA_INDEX_GROUPID,
    INDEX_KAFKA_INDEX_COMMITPERIOD,
    INDEX_KAFKA_INDEX_AES,
    INDEX_KAFKA_INDEX_MAC,
*/
    
    INDEX_HBASE_DATA_ZKCONNECT,
    INDEX_HBASE_DATA_ZNODE,
    INDEX_HBASE_DATA_TABLE,
    INDEX_HBASE_DATA_COLFAM,
    //INDEX_HBASE_DATA_AES,

    INDEX_HBASE_METADATA_ZKCONNECT,
    INDEX_HBASE_METADATA_ZNODE,
    INDEX_HBASE_METADATA_TABLE,
    INDEX_HBASE_METADATA_COLFAM,
    INDEX_HBASE_METADATA_AES,

    INDEX_HBASE_INDEX_ZKCONNECT,
    INDEX_HBASE_INDEX_ZNODE,
    INDEX_HBASE_INDEX_TABLE,
    INDEX_HBASE_INDEX_COLFAM,
    INDEX_HBASE_INDEX_AES,
  };

  private final KeyStore keystore;
  
  /**
   * Modulus for selecting metadata
   */
  private final byte modulus;
  
  /**
   * Remainder to match for metadata to be selected.
   */
  private final byte remainder;
  
  /**
   * Pool for connections to the index HBase
   */
  private final HTablePool indexHTPool;
  
  /**
   * Name of HBase table containing the index specifications
   */
  private final String indexTable;
  
  /**
   * Column family for index specifications
   */
  private final byte[] indexColfam;
  
  /**
   * Pool for connections to the metadata HBase
   */
  private final HTablePool metadataHTPool;
  
  /**
   * Name of HBase table containing the metadata
   */
  private final String metadataTable;
  
  /**
   * Column family for metadata
   */
  private final byte[] metadataColfam;
  
  /**
   * Pool for connections to the data HBase
   */
  private final HTablePool dataHTPool;
  
  /**
   * Name of HBase table containing the data
   */
  private final String dataTable;
  
  /**
   * Column family for data
   */
  private final byte[] dataColfam;
  
  /**
   * Map of owner to associated index specifications
   */
  private final Map<String,List<IndexSpec>> indicesByOwner = new ConcurrentHashMap<String, List<IndexSpec>>();
  
  /**
   * Map of class'labels selection pattern by index id
   */
  private final Map<Long, Map<String,Pattern>> selectorsByIndexId = new ConcurrentHashMap<Long, Map<String,Pattern>>();
  
  /**
   * Indices to apply to a given GTS instance
   */
  private final Map<BigInteger, List<IndexSpec>> indicesByGTS = new ConcurrentHashMap<BigInteger, List<IndexSpec>>();
  
  private final AtomicBoolean indexLoaded = new AtomicBoolean(false);
  private final AtomicBoolean metadataLoaded = new AtomicBoolean(false);

  private final Properties properties;

  /**
   * Flag indicating that the data consumers should be killed and respawned
   */
  private final AtomicBoolean abort = new AtomicBoolean(false);
  
  /**
   * Synchronization barrier for the DataConsumers
   */
  private CyclicBarrier barrier;
  
  public Index(KeyStore keystore, Properties props) {    
    this.properties = (Properties) props.clone();
    
    //
    // Check required properties
    //
    
    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);          
    }

    //
    // Extract parameters
    //
    
    String partition = properties.getProperty(INDEX_PARTITION);
    String[] tokens = partition.split(":");
    this.modulus = Byte.valueOf(tokens[0]);
    this.remainder = Byte.valueOf(tokens[1]);

    this.keystore = keystore;
    
    //
    // Create HTablePool instances
    //
    
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", properties.getProperty(INDEX_HBASE_INDEX_ZKCONNECT));
    if (!"".equals(properties.getProperty(INDEX_HBASE_INDEX_ZNODE))) {
      conf.set("zookeeper.znode.parent", properties.getProperty(INDEX_HBASE_INDEX_ZNODE));
    }
    this.indexHTPool = new HTablePool(conf, 1);
    this.indexTable = properties.getProperty(INDEX_HBASE_INDEX_TABLE);
    this.indexColfam = properties.getProperty(INDEX_HBASE_INDEX_COLFAM).getBytes(Charsets.UTF_8);
    
    conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", properties.getProperty(INDEX_HBASE_METADATA_ZKCONNECT));
    if (!"".equals(properties.getProperty(INDEX_HBASE_METADATA_ZNODE))) {
      conf.set("zookeeper.znode.parent", properties.getProperty(INDEX_HBASE_METADATA_ZNODE));
    }
    this.metadataHTPool = new HTablePool(conf, 1);
    this.metadataTable = properties.getProperty(INDEX_HBASE_METADATA_TABLE);
    this.metadataColfam = properties.getProperty(INDEX_HBASE_METADATA_COLFAM).getBytes(Charsets.UTF_8);

    conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", properties.getProperty(INDEX_HBASE_DATA_ZKCONNECT));
    if (!"".equals(properties.getProperty(INDEX_HBASE_DATA_ZNODE))) {
      conf.set("zookeeper.znode.parent", properties.getProperty(INDEX_HBASE_DATA_ZNODE));
    }
    this.dataHTPool = new HTablePool(conf, 1);
    this.dataTable = properties.getProperty(INDEX_HBASE_DATA_TABLE);
    this.dataColfam = properties.getProperty(INDEX_HBASE_DATA_COLFAM).getBytes(Charsets.UTF_8);

    //
    // Extract crypto keys
    //
    
    extractKeys(properties);
    
    //
    // Launch the Index retrieval Thread
    //
    
    Thread indexRetriever = new IndexIndexspecConsumer(this);
    indexRetriever.setName("Continuum Index IndexSpecConsumer");
    indexRetriever.setDaemon(true);
    indexRetriever.start();
    
    //
    // Launch the metadata retrieval Thread
    //
    
    Thread metadataRetriever = new IndexMetadataConsumer(this);
    metadataRetriever.setName("Continuum Index MetadataConsumer");
    metadataRetriever.setDaemon(true);
    metadataRetriever.start();
    
    //
    // Launch the coordination Thread
    //
    
    Thread coordinator = new IndexCoordinator(this);
    coordinator.setName("Continuum Index Coordinator");
    coordinator.setDaemon(true);
    coordinator.start();
    
  }
  
  
  /**
   * Extract Index related keys and populate the KeyStore with them.
   * 
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(Properties props) {
    String keyspec = props.getProperty(INDEX_KAFKA_METADATA_MAC);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + INDEX_KAFKA_METADATA_MAC + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_KAFKA_METADATA, key);
    }

    keyspec = props.getProperty(INDEX_KAFKA_METADATA_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + INDEX_KAFKA_METADATA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_KAFKA_METADATA, key);
    }
    
    keyspec = props.getProperty(INDEX_HBASE_METADATA_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + INDEX_HBASE_METADATA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_HBASE_METADATA, key);
    }

    
    keyspec = props.getProperty(INDEX_KAFKA_DATA_MAC);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + INDEX_KAFKA_DATA_MAC + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_KAFKA_DATA, key);
    }

    keyspec = props.getProperty(INDEX_KAFKA_DATA_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + INDEX_KAFKA_DATA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_KAFKA_DATA, key);
    }
    
    keyspec = props.getProperty(INDEX_HBASE_DATA_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + INDEX_HBASE_DATA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_HBASE_DATA, key);
    }
    
    
    
    keyspec = props.getProperty(INDEX_KAFKA_INDEX_MAC);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + INDEX_KAFKA_INDEX_MAC + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_KAFKA_INDEX, key);
    }

    keyspec = props.getProperty(INDEX_KAFKA_INDEX_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + INDEX_KAFKA_INDEX_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_KAFKA_INDEX, key);
    }

    keyspec = props.getProperty(INDEX_HBASE_INDEX_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + INDEX_HBASE_INDEX_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_HBASE_INDEX, key);
    }

  }

  private static class IndexIndexspecConsumer extends Thread {
    
    private final Index index;
    
    public IndexIndexspecConsumer(Index index) {
      this.index = index;
    }
    
    @Override
    public void run() {
      
      try {
        //
        // Read index specs from HBase, keeping only the matching ones
        //
        
        HTableInterface htable = index.indexHTPool.getTable(index.indexTable);

        Scan scan = new Scan();
        scan.setStartRow(HBASE_INDEX_KEY_PREFIX);
        // FIXME(hbs): we know the prefix is 'I', so we use 'J' as the stoprow
        scan.setStopRow("J".getBytes(Charsets.UTF_8));
        scan.addFamily(index.indexColfam);
        scan.setCaching(1000);
        scan.setBatch(1000);
        AESWrapEngine engine = new AESWrapEngine();
        CipherParameters params = new KeyParameter(index.keystore.getKey(KeyStore.AES_HBASE_INDEX));
        engine.init(false, params);

        PKCS7Padding padding = new PKCS7Padding();

        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        ResultScanner scanner = htable.getScanner(scan);
        
        byte[] EMPTY_COLQ = new byte[0];
        
        do {
          Result result = scanner.next();
          
          if (null == result) {
            break;
          }
                    
          byte[] value = result.getValue(index.indexColfam, EMPTY_COLQ);
          
          //
          // Unwrap
          //
          
          byte[] unwrapped = engine.unwrap(value, 0, value.length);
          
          //
          // Unpad
          //
          
          int padcount = padding.padCount(unwrapped);
          byte[] unpadded = Arrays.copyOf(unwrapped, unwrapped.length - padcount);
          
          //
          // Deserialize
          //

          IndexSpec spec = new IndexSpec();
          deserializer.deserialize(spec, unpadded);
          
          //
          // Compute indexId and compare it to the value in the row key
          //

          long indexId = IndexUtil.indexId(index.keystore, spec);
          
          ByteBuffer bb = ByteBuffer.wrap(result.getRow()).order(ByteOrder.BIG_ENDIAN);
          bb.position(1);
          long hbIndexId = bb.getLong();
          
          // If classId/labelsId are incoherent, skip metadata
          if (indexId != hbIndexId) {
            // FIXME(hbs): LOG
            System.err.println("Incoherent index Id for " + spec);
            continue;
          }
    
          // Force indexId
          spec.setIndexId(indexId);
          
          // Add binary components for key prefix and index id
          IndexComponent indexIdComponent = new IndexComponent(IndexComponentType.BINARY);
          indexIdComponent.setKey(Longs.toByteArray(indexId));
          spec.getComponents().add(0, indexIdComponent);
          
          IndexComponent prefixComponent = new IndexComponent(IndexComponentType.BINARY);
          prefixComponent.setKey(HBASE_INDEX_KEY_PREFIX);
          spec.getComponents().add(0, prefixComponent);
          
          // Compute index key size
          spec.setKeyLength(IndexUtil.getKeyLength(spec));
          
          //
          // Compute class selection pattern
          //
          
          Map<String,Pattern> patterns = null;
          
          try {
            patterns = GTSHelper.patternsFromSelectors(spec.getSelector());
          } catch (Exception e) {
            // FIXME(hbs): LOG
            System.err.println("Error extracting selection patterns");
            continue;
          }

          index.selectorsByIndexId.put(indexId, patterns);
          
          //
          // Store index spec under its owner
          //
          
          if (!index.indicesByOwner.containsKey(spec.getOwner())) {
            index.indicesByOwner.put(spec.getOwner(), new ArrayList<IndexSpec>());
          }
          
          index.indicesByOwner.get(spec.getOwner()).add(spec);
          
        } while (true);
        
        htable.close();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      } catch (InvalidCipherTextException icte) {
        throw new RuntimeException(icte);
      } catch (TException te) {
        throw new RuntimeException(te);
      } finally {
        index.indexLoaded.set(true);
      }

      //
      // Now consume index specifications from Kafka
      //
      
      
    }
  }
  
  private static final class IndexMetadataConsumer extends Thread {
    
    private final Index index;
    
    public IndexMetadataConsumer(Index index) {
      this.index = index;
    }
    
    @Override
    public void run() {
      
      //
      // Wait for the indices to be loaded
      //
      
      while (!index.indexLoaded.get()) {
        try { Thread.sleep(10L); } catch (InterruptedException ie) {}
      }
      
      try {
        //
        // Populate the metadata cache with initial data from HBase
        //
        
        HTableInterface htable = index.metadataHTPool.getTable(index.metadataTable);

        Scan scan = new Scan();
        scan.setStartRow(DirectoryRowFilter.HBASE_METADATA_KEY_PREFIX);
        // FIXME(hbs): we know the prefix is 'M', so we use 'N' as the stoprow
        scan.setStopRow("N".getBytes(Charsets.UTF_8));
        scan.addFamily(index.metadataColfam);
        scan.setCaching(1000);
        scan.setBatch(1000);
        
        // FIXME(hbs): add a filter to only return the rows that should be selected by the modulus/remainder pair
        
        AESWrapEngine engine = new AESWrapEngine();
        CipherParameters params = new KeyParameter(index.keystore.getKey(KeyStore.AES_HBASE_METADATA));
        engine.init(false, params);

        PKCS7Padding padding = new PKCS7Padding();

        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        ResultScanner scanner = htable.getScanner(scan);
        
        byte[] EMPTY_COLQ = new byte[0];
        
        do {
          Result result = scanner.next();
          if (null == result) {
            break;
          }
          
          // We check the first byte of the labels Id for determining if we should keep the metadata or not
          // This is because there will be more labelsId than classId (this is an educated guess...)
          byte r = (byte) (result.getRow()[DirectoryRowFilter.HBASE_METADATA_KEY_PREFIX.length + 8] % index.modulus);
          
          // Skip metadata if its modulus is not the one we expect
          if (index.remainder != r) {
            continue;
          }
          
          byte[] value = result.getValue(index.metadataColfam, EMPTY_COLQ);
          
          //
          // Unwrap
          //
          
          byte[] unwrapped = engine.unwrap(value, 0, value.length);
          
          //
          // Unpad
          //
          
          int padcount = padding.padCount(unwrapped);
          byte[] unpadded = Arrays.copyOf(unwrapped, unwrapped.length - padcount);
          
          //
          // Deserialize
          //

          Metadata metadata = new Metadata();
          deserializer.deserialize(metadata, unpadded);
          
          //
          // Compute classId/labelsId and compare it to the values in the row key
          //
          
          long classId = GTSHelper.classId(index.keystore.getKey(KeyStore.SIPHASH_CLASS), metadata.getName());
          long labelsId = GTSHelper.labelsId(index.keystore.getKey(KeyStore.SIPHASH_LABELS), metadata.getLabels());
          
          ByteBuffer bb = ByteBuffer.wrap(result.getRow()).order(ByteOrder.BIG_ENDIAN);
          bb.position(DirectoryRowFilter.HBASE_METADATA_KEY_PREFIX.length);
          long hbClassId = bb.getLong();
          long hbLabelsId = bb.getLong();
          
          // If classId/labelsId are incoherent, skip metadata
          if (classId != hbClassId || labelsId != hbLabelsId) {
            // FIXME(hbs): LOG
            System.err.println("Incoherent class/labels Id for " + metadata);
            continue;
          }

          //
          // Force class/labels Id
          //
          metadata.setClassId(classId);
          metadata.setLabelsId(labelsId);

          //
          // Register the indices for the GTS describe by 'metadata'
          //
          
          registerGTSIndices(metadata);
          
        } while (true);
        
        htable.close();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      } catch (InvalidCipherTextException icte) {
        throw new RuntimeException(icte);
      } catch (TException te) {
        throw new RuntimeException(te);
      } finally {
        index.metadataLoaded.set(true);
      }
      
      //
      // Now consume the 'metadata' topic and register the indices for any new metadata
      //

      while(true) {
        try {
          Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
          
          topicCountMap.put(index.properties.getProperty(INDEX_KAFKA_METADATA_TOPIC), 1);
          
          Properties props = new Properties();
          props.setProperty("zookeeper.connect", index.properties.getProperty(INDEX_KAFKA_METADATA_ZKCONNECT));
          props.setProperty("group.id", index.properties.getProperty(INDEX_KAFKA_METADATA_GROUPID));
          props.setProperty("auto.commit.enable", "false");    
          
          ConsumerConfig config = new ConsumerConfig(props);
          ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
          
          Map<String,List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
          
          List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(index.properties.getProperty(INDEX_KAFKA_METADATA_TOPIC));

          long lastCommit = System.currentTimeMillis();
          long commitPeriod = Long.valueOf(index.properties.getProperty(INDEX_KAFKA_METADATA_COMMITPERIOD));
          
          try {
            ConsumerIterator<byte[],byte[]> iter = streams.get(0).iterator();

            while(iter.hasNext()) {
              MessageAndMetadata<byte[], byte[]> msg = iter.next();
              
              byte[] data = msg.message();
              
              if (null != index.keystore.getKey(KeyStore.SIPHASH_KAFKA_METADATA)) {
                data = CryptoUtils.removeMAC(index.keystore.getKey(KeyStore.SIPHASH_KAFKA_METADATA), data);
              }
              
              // Skip data whose MAC was not verified successfully
              if (null == data) {
                // TODO(hbs): increment Sensision metric
                continue;
              }
              
              // Unwrap data if need be
              if (null != index.keystore.getKey(KeyStore.AES_KAFKA_METADATA)) {
                data = CryptoUtils.unwrap(index.keystore.getKey(KeyStore.AES_KAFKA_METADATA), data);
              }
              
              // Skip data that was not unwrapped successfully
              if (null == data) {
                // TODO(hbs): increment Sensision metric
                continue;
              }
              
              //
              // Only retain data with matching key
              //
              
              byte r = (byte) (msg.key()[8] % index.modulus);
              
              if (index.remainder != r) {
                continue;
              }
              
              //
              // TODO(hbs): We could check that metadata class/labels Id match those of the key, but
              // since it was wrapped/authenticated, we suppose it's ok.
              //
                          
              byte[] metadataBytes = Arrays.copyOfRange(data, 16, data.length);
              TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
              Metadata metadata = new Metadata();
              deserializer.deserialize(metadata, metadataBytes);
              
              //
              // Register indices for 'metadata'
              //
              
              registerGTSIndices(metadata);
              
              //
              // Commit offsets if that time has come
              //
              
              if (System.currentTimeMillis() - lastCommit > commitPeriod) {
                connector.commitOffsets();
                lastCommit = System.currentTimeMillis();
              }
            }
          } catch (Exception e) {
            
          } finally {
            if (null != connector) {
              connector.shutdown();
            }
          }          
        } catch (Exception e) {
        } finally {
          try { Thread.sleep(1000L); } catch (InterruptedException ie) {}
        }
      }
      
    }

    private void registerGTSIndices(Metadata metadata) {
      //
      // Extract the producer associated with the current metadata
      //
      
      String producer = metadata.getLabels().get(Constants.PRODUCER_LABEL);
      
      //
      // Extract the indices which belong to 'producer'
      //
      
      List<IndexSpec> indices = index.indicesByOwner.get(producer);
      
      if (null == indices) {
        return;
      }

      //
      // For each index, check if it should be applied to the GTS instance described by the current metadata.
      // If so, generate a custom IndexSpec to speed up indexing
      //
      
      for (IndexSpec spec: indices) {
        Map<String,Pattern> patterns = index.selectorsByIndexId.get(spec.getIndexId());
        
        //
        // Check if name matches (class pattern is associated with key 'null')
        //
        
        Matcher m = patterns.get(null).matcher(metadata.getName());
        
        if (!m.matches()) {
          continue;
        }
        
        //
        // Check if labels match
        //
        
        boolean matched = true;
        
        for (Entry<String,Pattern> entry: patterns.entrySet()) {
          // Skip class selector
          if (null == entry.getKey()) {
            continue;
          }
          // If metadata does not contain the current label, bail out
          if (!metadata.getLabels().containsKey(entry.getKey())) {
            matched = false;
            break;
          }
          
          Matcher matcher = entry.getValue().matcher(metadata.getLabels().get(entry.getKey()));
          
          if (!matcher.matches()) {
            matched = false;
            break;
          }
        }
        
        // Skip metadata which did not match
        if (!matched) {
          continue;
        }
        
        //
        // Create an indexSpec specifically for this GTS instance if need be (i.e. if there are index
        // components of type subclass or sublabels for which we need to compute the key component from
        // the metadata
        //
        
        IndexSpec gtsIndex = null;
        
        for (int i = 0; i < spec.getComponentsSize(); i++) {
          // If we need a custom IndexSpec for the GTS, populate its components
          if (null != gtsIndex) {
            if (IndexComponentType.SUBCLASS.equals(spec.getComponents().get(i).getType())) {
              // Delete content after 'level' levels in the class name
              int idx = -1;
              int count = 0;
              while(count < spec.getComponents().get(i).getLevels()) {
                idx = metadata.getName().indexOf(".", idx + 1);
                if (idx < 0) {
                  break;
                }
                count++;
              }
              
              // If idx is >= 0 then this means the count'th '.' exists, we
              // then remove anything including and after it
              
              String name = metadata.getName();
              
              if (idx >= 0) {
                name = name.substring(0, idx);
              }
              
              IndexComponent component = new IndexComponent(IndexComponentType.BINARY);
              component.setKey(Longs.toByteArray(GTSHelper.classId(index.keystore.getKey(KeyStore.SIPHASH_CLASS), name)));
              gtsIndex.addToComponents(component);
            } else if (IndexComponentType.SUBLABELS.equals(spec.getComponents().get(i).getType())) {
              // Extract the labels to be indexed
              Map<String,String> sublabels = new HashMap<String,String>();
              for (String label: spec.getComponents().get(i).getLabels()) {
                String labelValue = metadata.getLabels().get(label);
                if (null == labelValue) {
                  // Set the value of the label to the empty String if
                  sublabels.put(label, "");                      
                } else {
                  sublabels.put(label, labelValue);
                }
              }
              IndexComponent component = new IndexComponent(IndexComponentType.BINARY);
              component.setKey(Longs.toByteArray(GTSHelper.labelsId(index.keystore.getKey(KeyStore.SIPHASH_LABELS), sublabels)));
              gtsIndex.addToComponents(component);
            } else {
              // Copy the IndexComponent from the original one
              gtsIndex.addToComponents(spec.getComponents().get(i));
            }
          } else if (IndexComponentType.SUBCLASS.equals(spec.getComponents().get(i).getType())
                     || IndexComponentType.SUBLABELS.equals(spec.getComponents().get(i).getType())) {
            gtsIndex = new IndexSpec();
            // Copy indexId (we will need it in case we remove an index)
            gtsIndex.setIndexId(spec.getIndexId());
            gtsIndex.setKeyLength(spec.getKeyLength());
            // Copy the initial components verbatim
            for (int j = 0; j < i; j++) {
              gtsIndex.addToComponents(spec.getComponents().get(j));
            }
            // Decrement i so we re-enter the loop with the same value of i
            i--;
          }
        }
        
        if (null == gtsIndex) {
          gtsIndex = spec;
        }
        
        //
        // Store the index spec under classId/labelsId for fast retrieval when indexing the data
        //
        
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.BIG_ENDIAN);
        bb.putLong(metadata.getClassId());
        bb.putLong(metadata.getLabelsId());
        
        BigInteger gtsId = new BigInteger(bb.array());

        if (!index.indicesByGTS.containsKey(gtsId)) {
          index.indicesByGTS.put(gtsId, new ArrayList<IndexSpec>());
        }
        
        // If the number of indices associated with this gts is > 0, remove any
        // previous indexSpec with the same indexId as gtsIndex
        
        int idx = -1;
        if (index.indicesByGTS.get(gtsId).size() > 0) {
          for (int i = 0; i < index.indicesByGTS.get(gtsId).size(); i++) {
            if (index.indicesByGTS.get(gtsId).get(i).getIndexId() == gtsIndex.getIndexId()) {
              idx = i;
              break;
            }
          }
        }
        
        // Remove previous one
        if (idx > 0) {
          index.indicesByGTS.get(gtsId).remove(idx);
        }
        
        index.indicesByGTS.get(gtsId).add(gtsIndex);
      }
    }
  }
  
  private static final class IndexCoordinator extends Thread {
    
    private final Index index;
    
    public IndexCoordinator(Index index) {
      this.index = index;
    }
    
    @Override
    public void run() {
      
      //
      // Wait for the metadata to be loaded
      //
      
      while (!index.metadataLoaded.get()) {
        try { Thread.sleep(10L); } catch (InterruptedException ie) {}
      }

        //
        // Enter an endless loop which will spawn 'nthreads' threads
        // each time the Kafka consumer is shut down (which will happen if an error
        // happens while talking to HBase, to get a chance to re-read data from the
        // previous snapshot).
        //
        
        String topic = index.properties.getProperty(INDEX_KAFKA_DATA_TOPIC);
        int nthreads = Integer.valueOf(index.properties.getProperty(INDEX_KAFKA_DATA_NTHREADS));
        
        while (true) {
          try {
            Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
            
            topicCountMap.put(topic, nthreads);
            
            Properties props = new Properties();
            props.setProperty("zookeeper.connect", index.properties.getProperty(INDEX_KAFKA_DATA_ZKCONNECT));
            props.setProperty("group.id", index.properties.getProperty(INDEX_KAFKA_DATA_GROUPID));
            props.setProperty("auto.commit.enable", "false");    
            
            ConsumerConfig config = new ConsumerConfig(props);
            ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
            
            Map<String,List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
            
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

            index.barrier = new CyclicBarrier(streams.size() + 1);

            ExecutorService executor = Executors.newFixedThreadPool(nthreads);
            
            //
            // now create runnables which will consume messages
            //
            
            for (final KafkaStream<byte[],byte[]> stream : streams) {
              executor.submit(new IndexDataConsumer(this.index, stream));
            }      
            
            while(!index.abort.get()) {
              if (streams.size() == index.barrier.getNumberWaiting()) {
                //
                // Check if we should abort, which could happen when
                // an exception was thrown when flushing the commits just before
                // entering the barrier
                //
                
                if (index.abort.get()) {
                  break;
                }
                  
                //
                // All processing threads are waiting on the barrier, this means we can flush the offsets because
                // they have all processed data successfully for the given activity period
                //
                
                // Commit offsets
                connector.commitOffsets();
                
                // Release the waiting threads
                try {
                  index.barrier.await();
                } catch (Exception e) {
                  break;
                }
              }
              try {
                Thread.sleep(100L);          
              } catch (InterruptedException ie) {          
              }
            }

            //
            // We exited the loop, this means one of the threads triggered an abort,
            // we will shut down the executor and shut down the connector to start over.
            //
            
            executor.shutdownNow();
            connector.shutdown();
            index.abort.set(false);
          } catch (Throwable t) {
            t.printStackTrace(System.out);
          } finally {
            try {Thread.sleep(1000L);} catch (InterruptedException ie) {}
          }
        }
    }
  }
  
  private static final class IndexDataConsumer extends Thread {
    
    private final Index index;
    private final KafkaStream<byte[],byte[]> stream;
    
    public IndexDataConsumer(Index index, KafkaStream<byte[], byte[]> stream) {
      this.index = index;
      this.stream = stream;
    }
    
    @Override
    public void run() {
      HTableInterface htable = null;

      long count = 0L;
      
      try {
        ConsumerIterator<byte[],byte[]> iter = this.stream.iterator();

        byte[] siphashKey = index.keystore.getKey(KeyStore.SIPHASH_KAFKA_DATA);
        byte[] aesKey = index.keystore.getKey(KeyStore.AES_KAFKA_DATA);
                    
        htable = index.dataHTPool.getTable(index.dataTable);
        htable.setAutoFlush(false, true);

        final HTableInterface ht = htable;

        //
        // AtomicLong with the timestamp of the last Put or 0 if
        // none were added since the last flush
        //
        
        final AtomicLong lastPut = new AtomicLong(0L);
        
        final long dataCommitPeriod = Long.valueOf(index.properties.getProperty(INDEX_KAFKA_DATA_COMMITPERIOD));
        
        //
        // Start the synchronization Thread
        //
        
        Thread synchronizer = new Thread(new Runnable() {
          @Override
          public void run() {
            long lastsync = System.currentTimeMillis();
            
            //
            // Check for how long we've been storing readings, if we've reached the commitperiod,
            // flush any pending commits and synchronize with the other threads so offsets can be committed
            //

            while(true) { 
              long now = System.currentTimeMillis();
              
              if (now - lastsync > dataCommitPeriod) {
                //
                // We synchronize on 'ht' so the main Thread does not add Puts to ht
                //
                
                synchronized (ht) {
                  //
                  // Attempt to flush
                  //
                  
                  try {
                    ht.flushCommits();
                  } catch (IOException ioe) {
                    // If an exception is thrown, abort
                    index.abort.set(true);
                    return;
                  }        
                  
                  //
                  // Now join the cyclic barrier which will trigger the
                  // commit of offsets
                  //
                  
                  try {
                    index.barrier.await();
                  } catch (Exception e) {
                    index.abort.set(true);
                    return;
                  } finally {
                    lastsync = System.currentTimeMillis();
                  }
                }
              } else if (0 != lastPut.get() && (now - lastPut.get() > 500)) {
                //
                // If the last Put was added to 'ht' more than 500ms ago, force a flush
                //
                
                synchronized(ht) {
                  try {
                    ht.flushCommits();
                    // Reset lastPut to 0
                    lastPut.set(0L);
                  } catch (IOException ioe) {
                    index.abort.set(true);
                    return;
                  }                  
                }
              }
 
              try {
                Thread.sleep(100L);
              } catch (InterruptedException ie) {                
              }
            }
          }
        });
        synchronizer.setName("Continuum Index Synchronizer");
        synchronizer.setDaemon(true);
        synchronizer.start();
        
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
            if (count % 100000 == 0) {
              System.out.println("INDEX [" + System.currentTimeMillis() + "] >>> " + count);
            }
            
            MessageAndMetadata<byte[], byte[]> msg = iter.next();
            
            byte[] data = msg.message();
            
            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }
            
            // Skip data whose MAC was not verified successfully
            if (null == data) {
              // TODO(hbs): increment Sensision metric
              continue;
            }
            
            // Unwrap data if need be
            if (null != aesKey) {
              data = CryptoUtils.unwrap(aesKey, data);
            }
            
            // Skip data that was not unwrapped successfully
            if (null == data) {
              // TODO(hbs): increment Sensision metric
              continue;
            }
        
            KafkaDataMessage tmsg = new KafkaDataMessage();
            deserializer.deserialize(tmsg, data);
            
            if (null == tmsg.getData() || 0 == tmsg.getData().length) {
              continue;
            }
            
            //
            // Extract classId, labelsId and determine if it should be indexed or not
            //
            
            byte[] classId = Longs.toByteArray(tmsg.getClassId());
            byte[] labelsId = Longs.toByteArray(tmsg.getLabelsId());

            byte[] gtsIdBytes = new byte[16];
            System.arraycopy(classId, 0, gtsIdBytes, 0, 8);
            System.arraycopy(labelsId, 0, gtsIdBytes, 8, 8);
            
            BigInteger gtsId = new BigInteger(gtsIdBytes);

            List<IndexSpec> indices = index.indicesByGTS.get(gtsId);
            
            // If there are no indices for the GTS, skip it
            if (null == indices || indices.isEmpty()) {
              continue;
            }
            
            //
            // Create a GTSDecoder with the given readings 
            //

            GTSDecoder decoder = new GTSDecoder(0L, ByteBuffer.wrap(tmsg.getData()));
            
            // We will store each reading separately, this makes readings storage idempotent
            // If BLOCK_ENCODING is enabled, prefix encoding will be used to shrink column qualifiers
            
            while(decoder.next()) {

              long basets = decoder.getTimestamp();
              GTSEncoder encoder = new GTSEncoder(basets, index.keystore.getKey(KeyStore.AES_HBASE_DATA));
              encoder.addValue(basets, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
              
              //
              // Loop on all indices for this GTS and create a Put for each one
              //
              
              // Extract data once as this might be a costly operation if encryption is used
              byte[] databytes =  encoder.getBytes();

              for (IndexSpec spec: indices) {
                // Allocate space for the key
                byte[] key = new byte[spec.getKeyLength()];
                ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
                             
                boolean hasTime = false;
                
                // Loop on index components and fill the key
                for (IndexComponent component: spec.getComponents()) {
                  switch (component.getType()) {
                    case CLASS:
                      bb.put(classId);
                      break;
                    case GEO:
                      // FIXME(hbs): we could speed up this process by having a 'hasGeo' flag in the
                      // index structure, we could check it right away, which would save at least 2 calls
                      // to ByteBuffer.put (prefix, indexId)
                      
                      // Bail out if data has no location
                      if (GeoTimeSerie.NO_LOCATION == decoder.getLocation()) {
                        key = null;
                        break;
                      }
                      bb.put(GeoXPLib.bytesFromGeoXPPoint(decoder.getLocation(), component.getResolution()));
                      break;
                    case LABELS:
                      bb.put(labelsId);
                      break;
                    case BINARY:
                    case SUBCLASS:
                    case SUBLABELS:
                      bb.put(component.getKey());
                      break;
                    case TIME:
                      hasTime = true;
                      bb.putLong(Long.MAX_VALUE - (decoder.getTimestamp() - (decoder.getTimestamp() % component.getModulus())));
                      break;                                            
                  }
                }
                
                // Do not proceed with writing the data if key has been set to null, meaning there was no geo
                // information and the current index requires it.
                if (null == key) {
                  continue;
                }
                
                Put put = new Put(key);
                
                //
                // If there was a time component in the index, the column qualifier
                // will be the timestamp bytes. Otherwise it will be empty, thus allowing
                // to have an index with just the last received value.
                //
                
                if (hasTime) {
                  byte[] tsbytes = Longs.toByteArray(Long.MAX_VALUE - decoder.getTimestamp());
                  put.add(index.dataColfam, tsbytes, databytes);
                } else {
                  // We need to re-encode the decoded value with a base timestamp set to 0 instead of the current ts
                  encoder = new GTSEncoder(0L, index.keystore.getKey(KeyStore.AES_HBASE_DATA));
                  encoder.addValue(basets, decoder.getLocation(), decoder.getElevation(), decoder.getValue());                  
                  put.add(index.dataColfam, Constants.EMPTY_COLQ, encoder.getBytes());
                }
                
                try {
                  synchronized (ht) {
                    htable.put(put);
                    lastPut.set(System.currentTimeMillis());
                  }                
                } catch (IOException ioe) {
                  index.abort.set(true);
                  return;
                }            
              }              
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
        t.printStackTrace(System.out);
      } finally {
        // Set abort to true in case we exit the 'run' method
        index.abort.set(true);
        if (null != htable) {
          try { htable.close(); } catch (IOException ioe) {}
        }
      }
    }
  }
}
