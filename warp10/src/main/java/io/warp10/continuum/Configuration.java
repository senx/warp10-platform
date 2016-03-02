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

public class Configuration {

  public static final String OSS_MASTER_KEY = "oss.master.key";

  public static final String WARP_COMPONENTS = "warp.components";

  public static final String WARP_TOKEN_FILE = "warp.token.file";
  
  public static final String WARP_HASH_CLASS = "warp.hash.class";
  public static final String WARP_HASH_LABELS = "warp.hash.labels";
  public static final String CONTINUUM_HASH_INDEX = "warp.hash.index";
  public static final String WARP_HASH_TOKEN = "warp.hash.token";
  public static final String WARP_HASH_APP = "warp.hash.app";
  public static final String WARP_AES_TOKEN = "warp.aes.token";
  public static final String WARP_AES_SCRIPTS = "warp.aes.scripts";
  public static final String WARP_AES_LOGGING = "warp.aes.logging";
  public static final String WARP_DEFAULT_AES_LOGGING = "hex:3cf5cee9eadddba796f2cce0762f308ad9df36f4883841e167dab2889bcf215b";

  public static final String WARP_IDENT = "warp.ident";
  
  /**
   * Comma separated list of additional languages to support within WarpScript
   * This MUST be set as a system property
   */
  public static final String CONFIG_WARPSCRIPT_LANGUAGES = "warpscript.languages";
  
  public static final String WARPSCRIPT_MAX_OPS = "warpscript.maxops";
  public static final String WARPSCRIPT_MAX_BUCKETS = "warpscript.maxbuckets";
  public static final String WARPSCRIPT_MAX_DEPTH = "warpscript.maxdepth";
  public static final String WARPSCRIPT_MAX_FETCH = "warpscript.maxfetch";
  public static final String WARPSCRIPT_MAX_GTS = "warpscript.maxgts";
  public static final String WARPSCRIPT_MAX_LOOP_DURATION = "warpscript.maxloop";
  public static final String WARPSCRIPT_MAX_RECURSION = "warpscript.maxrecursion";
  public static final String WARPSCRIPT_MAX_SYMBOLS = "warpscript.maxsymbols";
  public static final String WARPSCRIPT_MAX_WEBCALLS = "warpscript.maxwebcalls";
  public static final String WARPSCRIPT_MAX_PIXELS = "warpscript.maxpixels";
  public static final String WARPSCRIPT_URLFETCH_LIMIT = "warpscript.urlfetch.limit";
  public static final String WARPSCRIPT_URLFETCH_MAXSIZE = "warpscript.urlfetch.maxsize";

  // Hard limits for the above limits which can be changed via a function call
  public static final String WARPSCRIPT_MAX_OPS_HARD = "warpscript.maxops.hard";
  public static final String WARPSCRIPT_MAX_BUCKETS_HARD = "warpscript.maxbuckets.hard";
  public static final String WARPSCRIPT_MAX_DEPTH_HARD = "warpscript.maxdepth.hard";
  public static final String WARPSCRIPT_MAX_FETCH_HARD = "warpscript.maxfetch.hard";
  public static final String WARPSCRIPT_MAX_GTS_HARD = "warpscript.maxgts.hard";
  public static final String WARPSCRIPT_MAX_LOOP_DURATION_HARD = "warpscript.maxloop.hard";
  public static final String WARPSCRIPT_MAX_RECURSION_HARD = "warpscript.maxrecursion.hard";
  public static final String WARPSCRIPT_MAX_SYMBOLS_HARD = "warpscript.maxsymbols.hard";
  public static final String WARPSCRIPT_MAX_PIXELS_HARD = "warpscript.maxpixels.hard";
  public static final String WARPSCRIPT_URLFETCH_LIMIT_HARD = "warpscript.urlfetch.limit.hard";
  public static final String WARPSCRIPT_URLFETCH_MAXSIZE_HARD = "warpscript.urlfetch.maxsize.hard";

  public static final String WEBCALL_USER_AGENT = "webcall.user.agent";

  /**
   * List of patterns to include/exclude for hosts in WebCall calls
   *
   * Typical value is .*,!^127.0.0.1$,!^localhost$,!^192.168.*,!^10.*,!^172.(16|17|18|19|20|21|22|23|24|25|26|27|28|29|39|31)\..*
   * 
   */
  public static final String WEBCALL_HOST_PATTERNS = "webcall.host.patterns";
  
  /**
   * Number of continuum time units per millisecond
   * 1000000 means we store nanoseconds
   * 1000 means we store microseconds
   * 1 means we store milliseconds
   * 0.001 means we store seconds (N/A since we use a long for the constant)
   */
  public static final String WARP_TIME_UNITS = "warp.timeunits";

  /**
   * Path of the 'bootstrap' Einstein code for Egress
   */
  public static final String CONFIG_WARPSCRIPT_BOOTSTRAP_PATH = "warpscript.bootstrap.path";
  
  /**
   * How often to reload the bootstrap code (in ms) for Egress
   */
  public static final String CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD = "warpscript.bootstrap.period";

  /**
   * Path of the 'bootstrap' Einstein code for Mobius
   */
  public static final String CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PATH = "warpscript.mobius.bootstrap.path";
  
  /**
   * How often to reload the bootstrap code (in ms) for Mobius
   */
  public static final String CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PERIOD = "warpscript.mobius.bootstrap.period";

  /**
   * Path of the 'bootstrap' Einstein code for Runner
   */
  public static final String CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PATH = "warpscript.runner.bootstrap.path";
  
  /**
   * How often to reload the bootstrap code (in ms) for Mobius
   */
  public static final String CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PERIOD = "warpscript.runner.bootstrap.period";

  /**
   * URL for the 'update' endpoint
   */
  public static final String CONFIG_WARPSCRIPT_UPDATE_ENDPOINT = "warpscript.update.endpoint";
  
  /**
   * URL for the 'meta' endpoint
   */
  public static final String CONFIG_WARPSCRIPT_META_ENDPOINT = "warpscript.meta.endpoint";

  /**
   * Pre-Shared key for signing fetch requests. Signed fetch request expose owner/producer
   */
  public static final String CONFIG_FETCH_PSK = "fetch.psk";

  /**
   * Maximum number of classes for which to report detailed stats in 'stats'
   */
  public static String DIRECTORY_STATS_CLASS_MAXCARDINALITY = "directory.stats.class.maxcardinality";
  
  /**
   * Maximum number of labels for which to report detailed stats in 'stats'
   */
  public static String DIRECTORY_STATS_LABELS_MAXCARDINALITY = "directory.stats.labels.maxcardinality";
  
  /**
   * Maximum size of Thrift frame for directory service
   */
  public static String DIRECTORY_FRAME_MAXLEN = "directory.frame.maxlen";

  /**
   * Maximum number of Metadata to return in find responses
   */
  public static String DIRECTORY_FIND_MAXRESULTS = "directory.find.maxresults";

  /**
   * Hard limit on number of find results. After this limit, the find request will fail.
   */
  public static String DIRECTORY_FIND_MAXRESULTS_HARD = "directory.find.maxresults.hard";
  
  /**
   * Zookeeper ZK connect string for Kafka ('metadata' topic)
   */  
  public static final String DIRECTORY_KAFKA_METADATA_ZKCONNECT = "directory.kafka.metadata.zkconnect";
  
  /**
   * Actual 'metadata' topic
   */
  public static final String DIRECTORY_KAFKA_METADATA_TOPIC = "directory.kafka.metadata.topic";
  
  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  public static final String DIRECTORY_KAFKA_METADATA_MAC = "directory.kafka.metadata.mac";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  public static final String DIRECTORY_KAFKA_METADATA_AES = "directory.kafka.metadata.aes";

  /**
   * Key to use for encrypting metadata in HBase (128/192/256 bits in hex or OSS reference) 
   */
  public static final String DIRECTORY_HBASE_METADATA_AES = "directory.hbase.metadata.aes";

  /**
   * Kafka group id with which to consume the metadata topic
   */
  public static final String DIRECTORY_KAFKA_METADATA_GROUPID = "directory.kafka.metadata.groupid";

  /**
   * Delay between synchronization for offset commit
   */
  public static final String DIRECTORY_KAFKA_METADATA_COMMITPERIOD = "directory.kafka.metadata.commitperiod";

  /**
   * Maximum byte size we allow the pending Puts list to grow to
   */
  public static final String DIRECTORY_HBASE_METADATA_MAXPENDINGPUTSSIZE = "directory.hbase.metadata.pendingputs.size";
  
  /**
   * ZooKeeper Quorum for locating HBase
   */
  public static final String DIRECTORY_HBASE_METADATA_ZKCONNECT = "directory.hbase.metadata.zkconnect";
  
  /**
   * HBase table where metadata should be stored
   */
  public static final String DIRECTORY_HBASE_METADATA_TABLE = "directory.hbase.metadata.table";
  
  /**
   * Columns family under which metadata should be stored
   */
  public static final String DIRECTORY_HBASE_METADATA_COLFAM = "directory.hbase.metadata.colfam";
  
  /**
   * Parent znode under which HBase znodes will be created
   */
  public static final String DIRECTORY_HBASE_METADATA_ZNODE = "directory.hbase.metadata.znode";

  /**
   * ZooKeeper server list for registering
   */
  public static final String DIRECTORY_ZK_QUORUM = "directory.zk.quorum";
  
  /**
   * ZooKeeper znode under which to register
   */
  public static final String DIRECTORY_ZK_ZNODE = "directory.zk.znode";
  
  /**
   * Number of threads to run for ingesting metadata from Kafka
   */
  public static final String DIRECTORY_KAFKA_NTHREADS = "directory.kafka.nthreads";

  /**
   * Number of threads to run for serving directory requests
   */
  public static final String DIRECTORY_SERVICE_NTHREADS = "directory.service.nthreads";

  /**
   * Partition of metadatas we focus on, format is MODULUS:REMAINDER
   */
  public static final String DIRECTORY_PARTITION = "directory.partition";
  
  /**
   * Port on which the DirectoryService will listen
   */
  public static final String DIRECTORY_PORT = "directory.port";
  
  /**
   * Port the streaming directory service listens to
   */
  public static final String DIRECTORY_STREAMING_PORT = "directory.streaming.port";

  /**
   * Number of Jetty selectors for the streaming server
   */
  public static final String DIRECTORY_STREAMING_SELECTORS = "directory.streaming.selectors";

  /**
   * Number of Jetty acceptors for the streaming server
   */
  public static final String DIRECTORY_STREAMING_ACCEPTORS = "directory.streaming.acceptors";

  /**
   * Idle timeout for the streaming directory endpoint
   */
  public static final String DIRECTORY_STREAMING_IDLE_TIMEOUT = "directory.streaming.idle.timeout";
  
  /**
   * Number of threads in Jetty's Thread Pool
   */
  public static final String DIRECTORY_STREAMING_THREADPOOL = "directory.streaming.threadpool";
  
  /**
   * Maximum size of Jetty ThreadPool queue size (unbounded by default)
   */
  public static final String DIRECTORY_STREAMING_MAXQUEUESIZE = "directory.streaming.maxqueuesize";

  /**
   * Address on which the DirectoryService will listen
   */
  public static final String DIRECTORY_HOST = "directory.host";
  
  /**
   * Pre-Shared Key for request fingerprinting
   */
  public static final String DIRECTORY_PSK = "directory.psk";
  
  /**
   * Max age of Find requests
   */
  public static final String DIRECTORY_MAXAGE = "directory.maxage";

  /**
   * Number of threads to use for the initial loading of Metadata
   */
  public static final String DIRECTORY_INIT_NTHREADS = "directory.init.nthreads";
  
  /**
   * Boolean indicating whether or not we should initialized Directory by reading HBase
   */
  public static final String DIRECTORY_INIT = "directory.init";

  /**
   * Boolean indicating whether or not we should store in HBase metadata we get from Kafka
   */
  public static final String DIRECTORY_STORE = "directory.store";

  /**
   * Boolean indicating whether or not we should do deletions in HBase
   */
  public static final String DIRECTORY_DELETE = "directory.delete";

  /**
   * Boolean indicting whether or not we should register in ZK
   */
  public static final String DIRECTORY_REGISTER = "directory.register";
  
  /**
   * Class name of directory plugin to use
   */
  public static final String DIRECTORY_PLUGIN_CLASS = "directory.plugin.class";
  
  /**
   * Boolean indicating whether or not we should use the HBase filter when initializing
   */
  public static final String DIRECTORY_HBASE_FILTER = "directory.hbase.filter";
  
  //
  // I N G R E S S
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * Path where the metadata cache should be dumped
   */
  public static final String INGRESS_CACHE_DUMP_PATH = "ingress.cache.dump.path";
  
  /**
   * Maximum value size
   */
  public static final String INGRESS_VALUE_MAXSIZE = "ingress.value.maxsize";
  
  /**
   * Identification of Ingress as the Metadata source
   */
  public static final String INGRESS_METADATA_SOURCE = "ingress";
  
  /**
   * Identification of Ingress/Delete as the Metadata source
   */
  public static final String INGRESS_METADATA_DELETE_SOURCE = "delete";
  
  /**
   * Identification of Ingress Metadata Update endpoint source
   */
  public static final String INGRESS_METADATA_UPDATE_ENDPOINT = "ingress.metadata.update";
  
  /**
   * Host onto which the ingress server should listen
   */
  public static final String INGRESS_HOST = "ingress.host";
  
  /**
   * Port onto which the ingress server should listen
   */
  public static final String INGRESS_PORT = "ingress.port";
  
  /**
   * Size of metadata cache in number of entries
   */
  public static final String INGRESS_METADATA_CACHE_SIZE = "ingress.metadata.cache.size";
  
  /**
   * Number of acceptors
   */
  public static final String INGRESS_ACCEPTORS = "ingress.acceptors";
  
  /**
   * Number of selectors
   */
  public static final String INGRESS_SELECTORS = "ingress.selectors";
  
  /**
   * Idle timeout
   */
  public static final String INGRESS_IDLE_TIMEOUT = "ingress.idle.timeout";
  
  /**
   * Number of threads in Jetty's Thread Pool
   */
  public static final String INGRESS_JETTY_THREADPOOL = "ingress.jetty.threadpool";
  
  /**
   * Maximum size of Jetty ThreadPool queue size (unbounded by default)
   */
  public static final String INGRESS_JETTY_MAXQUEUESIZE = "ingress.jetty.maxqueuesize";
    
  /**
   * Max message size for the stream update websockets
   */
  public static final String INGRESS_WEBSOCKET_MAXMESSAGESIZE = "ingress.websocket.maxmessagesize";
  
  /**
   * ZooKeeper server list
   */
  public static final String INGRESS_ZK_QUORUM = "ingress.zk.quorum";
  
  /**
   * ZooKeeper znode under which to register
   */
  public static final String INGRESS_ZK_ZNODE = "ingress.zk.znode";
  
  /**
   * ZK Connect String for the metadata kafka cluster
   */
  public static final String INGRESS_KAFKA_META_ZKCONNECT = "ingress.kafka.metadata.zkconnect";
  
  /**
   * Kafka broker list for the 'meta' topic
   */
  public static final String INGRESS_KAFKA_META_BROKERLIST = "ingress.kafka.metadata.brokerlist";

  /**
   * Actual 'meta' topic
   */
  public static final String INGRESS_KAFKA_META_TOPIC = "ingress.kafka.metadata.topic";    

  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  public static final String INGRESS_KAFKA_META_MAC = "ingress.kafka.metadata.mac";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference)
   */
  public static final String INGRESS_KAFKA_META_AES = "ingress.kafka.metadata.aes";
  
  /**
   * Groupid to use for consuming the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_META_GROUPID = "ingress.kafka.metadata.groupid";

  /**
   * How often to commit the offsets for topic 'metadata' (in ms)
   */
  public static final String INGRESS_KAFKA_META_COMMITPERIOD = "ingress.kafka.metadata.commitperiod";

  /**
   * Number of threads to use for consuming the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_META_NTHREADS = "ingress.kafka.metadata.nthreads";

  /**
   * ZK Connect String for the data kafka cluster
   */
  public static final String INGRESS_KAFKA_DATA_ZKCONNECT = "ingress.kafka.data.zkconnect";
  
  /**
   * Kafka broker list for the 'data' topic
   */
  public static final String INGRESS_KAFKA_DATA_BROKERLIST = "ingress.kafka.data.brokerlist";
  
  /**
   * Actual 'data' topic
   */
  public static final String INGRESS_KAFKA_DATA_TOPIC = "ingress.kafka.data.topic";
  
  /**
   * Size of Kafka Producer pool for the 'data' topic
   */
  public static final String INGRESS_KAFKA_DATA_POOLSIZE = "ingress.kafka.data.poolsize";

  /**
   * Request timeout when talking to Kafka
   */
  public static final String INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS = "ingress.kafka.data.request.timeout.ms";
  
  /**
   * Size of Kafka Producer pool for the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_METADATA_POOLSIZE = "ingress.kafka.metadata.poolsize";

  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  public static final String INGRESS_KAFKA_DATA_MAC = "ingress.kafka.data.mac";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  public static final String INGRESS_KAFKA_DATA_AES = "ingress.kafka.data.aes";
  
  /**
   * Maximum message size for the 'data' topic
   */
  public static final String INGRESS_KAFKA_DATA_MAXSIZE = "ingress.kafka.data.maxsize";
  
  /**
   * Maximum message size for the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_METADATA_MAXSIZE = "ingress.kafka.metadata.maxsize";

  /**
   * ZK Connect String for the archive kafka cluster
   */
  public static final String INGRESS_KAFKA_ARCHIVE_ZKCONNECT = "ingress.kafka.archive.zkconnect";
  
  /**
   * Kafka broker list for the 'archive' topic
   */
  public static final String INGRESS_KAFKA_ARCHIVE_BROKERLIST = "ingress.kafka.archive.brokerlist";

  /**
   * Actual 'archive' topic
   */
  public static final String INGRESS_KAFKA_ARCHIVE_TOPIC = "ingress.kafka.archive.topic";

  /**
   * Key to use for computing archive requests MACs (128 bits in hex or OSS reference)
   */
  public static final String INGRESS_KAFKA_ARCHIVE_MAC = "ingress.kafka.archive.mac";

  /**
   * Key to use for encrypting archive payloads (128/192/256 bits in hex or OSS reference) 
   */
  public static final String INGRESS_KAFKA_ARCHIVE_AES = "ingress.kafka.archive.aes";
  
  //
  // S T O R E
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * Key for encrypting data in HBase
   */
  public static final String STORE_HBASE_DATA_AES = "store.hbase.data.aes";
  
  /**
   * Zookeeper ZK connect string for Kafka ('data' topic)
   */  
  public static final String STORE_KAFKA_DATA_ZKCONNECT = "store.kafka.data.zkconnect";
  
  /**
   * Kafka broker list for the 'data' topic
   */
  public static final String STORE_KAFKA_DATA_BROKERLIST = "store.kafka.data.brokerlist";
  
  /**
   * Actual 'data' topic
   */
  public static final String STORE_KAFKA_DATA_TOPIC = "store.kafka.data.topic";
  
  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  public static final String STORE_KAFKA_DATA_MAC = "store.kafka.data.mac";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  public static final String STORE_KAFKA_DATA_AES = "store.kafka.data.aes";

  /**
   * Kafka group id with which to consume the data topic
   */
  public static final String STORE_KAFKA_DATA_GROUPID = "store.kafka.data.groupid";

  /**
   * Delay between synchronization for offset commit
   */
  public static final String STORE_KAFKA_DATA_COMMITPERIOD = "store.kafka.data.commitperiod";

  /**
   * Maximum time between offset synchronization
   */
  public static final String STORE_KAFKA_DATA_INTERCOMMITS_MAXTIME = "store.kafka.data.intercommits.maxtime";
  
  /**
   * Maximum size we allow the Puts list to grow to
   */
  public static final String STORE_HBASE_DATA_MAXPENDINGPUTSSIZE = "store.hbase.data.maxpendingputssize";
  
  /**
   * How many threads to spawn for consuming
   */
  public static final String STORE_NTHREADS = "store.nthreads";
  
  /**
   * ZooKeeper server list
   */
  public static final String STORE_ZK_QUORUM = "store.zk.quorum";
  
  /**
   * ZooKeeper znode under which to register
   */
  public static final String STORE_ZK_ZNODE = "store.zk.znode";
  
  /**
   * ZooKeeper connect string for HBase
   */
  public static final String STORE_HBASE_DATA_ZKCONNECT = "store.hbase.data.zkconnect";
  
  /**
   * HBase table where data should be stored
   */
  public static final String STORE_HBASE_DATA_TABLE = "store.hbase.data.table";
  
  /**
   * Columns family under which data should be stored
   */
  public static final String STORE_HBASE_DATA_COLFAM = "store.hbase.data.colfam";
  
  /**
   * Parent znode under which HBase znodes will be created
   */
  public static final String STORE_HBASE_DATA_ZNODE = "store.hbase.data.znode";

  /**
   * Custom value of 'hbase.hconnection.threads.max' for the Store HBase pool
   */
  public static final String STORE_HBASE_HCONNECTION_THREADS_MAX = "store.hbase.hconnection.threads.max";
  
  /**
   * Custom value of 'hbase.hconnection.threads.core' for the Store HBase pool (MUST be <= STORE_HBASE_HCONNECTION_THREADS_MAX)
   */
  public static final String STORE_HBASE_HCONNECTION_THREADS_CORE = "store.hbase.hconnection.threads.core";
  
  //
  // P L A S M A
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * ZooKeeper connect string for Kafka consumer
   */
  public static final String PLASMA_FRONTEND_KAFKA_ZKCONNECT = "plasma.frontend.kafka.zkconnect";
  
  /**
   * Kafka topic to consume. This topic is dedicated to this Plasma frontend.
   */
  public static final String PLASMA_FRONTEND_KAFKA_TOPIC = "plasma.frontend.kafka.topic";
  
  /**
   * Kafka groupid under which to consume above topic
   */
  public static final String PLASMA_FRONTEND_KAFKA_GROUPID = "plasma.frontend.kafka.groupid";
  
  /**
   * How often (in ms) to commit Kafka offsets
   */
  public static final String PLASMA_FRONTEND_KAFKA_COMMITPERIOD = "plasma.frontend.kafka.commitperiod";
  
  /**
   * Number of threads used for consuming Kafka topic
   */
  public static final String PLASMA_FRONTEND_KAFKA_NTHREADS = "plasma.frontend.kafka.nthreads";
  
  /**
   * Optional AES key for messages in Kafka
   */
  public static final String PLASMA_FRONTEND_KAFKA_AES = "plasma.frontend.kafka.aes";
  
  /**
   * ZooKeeper connect String for subscription
   */
  public static final String PLASMA_FRONTEND_ZKCONNECT = "plasma.frontend.zkconnect";
  
  /**
   * ZooKeeper root znode for subscrptions
   */
  public static final String PLASMA_FRONTEND_ZNODE = "plasma.frontend.znode";
  
  /**
   * Maximum size of each znode (in bytes)
   */
  public static final String PLASMA_FRONTEND_MAXZNODESIZE = "plasma.frontend.maxznodesize";
  
  /**
   * Host/IP on which to bind
   */
  public static final String PLASMA_FRONTEND_HOST = "plasma.frontend.host";
  
  /**
   * Port on which to listen
   */
  public static final String PLASMA_FRONTEND_PORT = "plasma.frontend.port";
  
  /**
   * Number of acceptors
   */
  public static final String PLASMA_FRONTEND_ACCEPTORS = "plasma.frontend.acceptors";

  /**
   * Number of selectors
   */
  public static final String PLASMA_FRONTEND_SELECTORS = "plasma.frontend.selectors";

  /**
   * Max message size for the Plasma Frontend Websocket
   */
  public static final String PLASMA_FRONTEND_WEBSOCKET_MAXMESSAGESIZE = "plasma.frontend.websocket.maxmessagesize";
  
  /**
   * Idle timeout
   */
  public static final String PLASMA_FRONTEND_IDLE_TIMEOUT = "plasma.frontend.idle.timout";
  
  /**
   * SipHash key for computing MACs of Kafka messages
   */
  public static final String PLASMA_FRONTEND_KAFKA_MAC = "plasma.frontend.kafka.mac";
  
  public static final String PLASMA_FRONTEND_SUBSCRIBE_DELAY = "plasma.frontend.subscribe.delay";
  
  /**
   * Zookeeper ZK connect string for Kafka ('in' topic)
   */  
  public static final String PLASMA_BACKEND_KAFKA_IN_ZKCONNECT = "plasma.backend.kafka.in.zkconnect";
  
  /**
   * Actual 'in' topic
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_TOPIC = "plasma.backend.kafka.in.topic";
  
  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_MAC = "plasma.backend.kafka.in.mac";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_AES = "plasma.backend.kafka.in.aes";

  /**
   * Kafka group id with which to consume the in topic
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_GROUPID = "plasma.backend.kafka.in.groupid";

  /**
   * Delay between synchronization for offset commit
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_COMMITPERIOD = "plasma.backend.kafka.in.commitperiod";

  /**
   * Number of threads to run for reading off of Kafka
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_NTHREADS = "plasma.backend.kafka.in.nthreads";

  /**
   * Zookeeper ZK connect string for Kafka ('out' topic)
   */  
  public static final String PLASMA_BACKEND_KAFKA_OUT_ZKCONNECT = "plasma.backend.kafka.out.zkconnect";
  
  /**
   * Kafka broker list for the 'out' topic
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_BROKERLIST = "plasma.backend.kafka.out.brokerlist";

  /**
   * Maximum size of Kafka outward messages
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_MAXSIZE = "plasma.backend.kafka.out.maxsize";

  /**
   * Actual 'out' topic
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_TOPIC = "plasma.backend.kafka.out.topic";
  
  /**
   * Key to use for computing MACs (128 bits in hex or OSS reference)
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_MAC = "plasma.backend.kafka.out.mac";
  
  /**
   * Key to use for encrypting payloads (128/192/256 bits in hex or OSS reference) 
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_AES = "plasma.backend.kafka.out.aes";

  /**
   * ZooKeeper Quorum for the ZK ensemble to use for retrieving subscriptions
   */
  public static final String PLASMA_BACKEND_SUBSCRIPTIONS_ZKCONNECT = "plasma.backend.subscriptions.zkconnect";
  
  /**
   * Parent znode under which subscription znodes will be created
   */
  public static final String PLASMA_BACKEND_SUBSCRIPTIONS_ZNODE = "plasma.backend.subscriptions.znode";
    

  //
  // R U N N E R
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * ZooKeeper connect string for the leader election among schedulers
   */
  public static final String RUNNER_ZK_QUORUM = "runner.zk.quorum";
  
  /**
   * Znode to use for the leader election among schedulers
   */
  public static final String RUNNER_ZK_ZNODE = "runner.zk.znode";
  
  /**
   * String uniquely identifying this instance of ScriptRunner
   */
  public static final String RUNNER_ID = "runner.id";
  
  /**
   * Roles of the ScriptRunner instance. Can either be 'standalone' or any combination of 'scheduler' and 'worker'.
   */
  public static final String RUNNER_ROLES = "runner.roles";
  
  /**
   * Root directory under which scripts to run reside. The scripts MUST have a '.mc2' extension
   * and reside in subdirectories of this root directory whose name is the periodicity (in ms) at
   * which to run them.
   */
  public static final String RUNNER_ROOT = "runner.root";
  
  /**
   * Number of threads to use for running scripts.
   */
  public static final String RUNNER_NTHREADS = "runner.nthreads";
  
  /**
   * How often (in ms) to scan RUNNER_ROOT for new scripts
   */
  public static final String RUNNER_SCANPERIOD = "runner.scanperiod";
  
  /**
   * Einstein endpoint to use for executing the scripts
   */
  public static final String RUNNER_ENDPOINT = "runner.endpoint";
  
  /**
   * Minimum period at which a script can be scheduled. Any script scheduled
   * more often than that won't be run
   */
  public static final String RUNNER_MINPERIOD = "runner.minperiod";
  
  /**
   * ZooKeeper connect string for the Kafka cluster
   */
  public static final String RUNNER_KAFKA_ZKCONNECT = "runner.kafka.zkconnect";
  
  /**
   * List of Kafka brokers
   */
  public static final String RUNNER_KAFKA_BROKERLIST = "runner.kafka.brokerlist";
  
  /**
   * Size of Kafka producer pool
   */
  public static final String RUNNER_KAFKA_POOLSIZE = "runner.kafka.poolsize";
  
  /**
   * Topic to use to submit the scripts
   */
  public static final String RUNNER_KAFKA_TOPIC = "runner.kafka.topic";
  
  /**
   * Groupid to use when consuming scripts
   */
  public static final String RUNNER_KAFKA_GROUPID = "runner.kafka.groupid";
  
  /**
   * Number of threads to spawn to consume scripts
   */
  public static final String RUNNER_KAFKA_NTHREADS = "runner.kafka.nthreads";
  
  /**
   * Commit period for the script topic
   */
  public static final String RUNNER_KAFKA_COMMITPERIOD = "runner.kafka.commitperiod";
  
  /**
   * Key for integrity checks
   */
  public static final String RUNNER_KAFKA_MAC = "runner.kafka.mac";
  
  /**
   * Key for encryption of scripts on topic
   */
  public static final String RUNNER_KAFKA_AES = "runner.kafka.aes";

  //
  // S T A N D A L O N E
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * Directory where the leveldb files should be created
   */
  public static final String LEVELDB_HOME = "leveldb.home";
  
  /**
   * AES key to use for wrapping metadata prior to storage in leveldb
   */
  public static final String LEVELDB_METADATA_AES = "leveldb.metadata.aes";
  
  /**
   * AES key to use for wrapping datapoints prior to storage in leveldb
   */
  public static final String LEVELDB_DATA_AES = "leveldb.data.aes";
  
  /**
   * @deprecated
   * AES key to use for storing index details in leveldb
   */
  public static final String LEVELDB_INDEX_AES = "leveldb.index.aes";
  
  /**
   * Cache size for leveldb (in bytes)
   */
  public static final String LEVELDB_CACHE_SIZE = "leveldb.cache.size";
  
  /**
   * Compression type to use for leveldb (SNAPPY/NONE)
   */
  public static final String LEVELDB_COMPRESSION_TYPE = "leveldb.compression.type";
  
  /**
   * IP to bind to for listening to incoming connections. Use 0.0.0.0 to listen to all interfaces
   */
  public static final String STANDALONE_HOST = "standalone.host";

  /**
   * Port to bind to for listening to incoming connections.
   */
  public static final String STANDALONE_PORT = "standalone.port";

  /**
   * Number of Jetty acceptors
   */
  public static final String STANDALONE_ACCEPTORS = "standalone.acceptors";

  /**
   * Idle timeout
   */
  public static final String STANDALONE_IDLE_TIMEOUT = "standalone.idle.timeout";
  
  /**
   * Number of Jetty selectors
   */
  public static final String STANDALONE_SELECTORS = "standalone.selectors";

  /**
   * Maximum encoder size (in bytes) for internal data transfers. Use values from 64k to 512k
   */
  public static final String STANDALONE_MAX_ENCODER_SIZE = "standalone.max.encoder.size";

  /**
   * Path to a file to use for triggering compaction suspension to take snapshots
   */
  public static final String STANDALONE_SNAPSHOT_TRIGGER = "standalone.snapshot.trigger";
  
  /**
   * Path to a file to use for signaling that compactions are suspended
   */
  public static final String STANDALONE_SNAPSHOT_SIGNAL = "standalone.snapshot.signal";
  
  /**
   * Set to 'true' to indicate the instance will use memory only for storage. This type of instance is non persistent.
   */
  public static final String IN_MEMORY = "in.memory";
  
  /**
   * Depth of timestamps to retain (in ms)
   */
  public static final String IN_MEMORY_DEPTH = "in.memory.depth";
  
  /**
   * High water mark in bytes. When memory goes above this threshold, attempts to remove expired datapoints will be
   * done until consumed memory goes below the low water mark (see below) or no more expired datapoints can be found.
   */
  public static final String IN_MEMORY_HIGHWATERMARK = "in.memory.highwatermark";
  
  /**
   * Low water mark in bytes for garbage collection (see above)
   */
  public static final String IN_MEMORY_LOWWATERMARK = "in.memory.lowwatermark";
  
  /**
   * If set to true, then only the last recorded value of a GTS is kept
   */
  public static final String IN_MEMORY_EPHEMERAL = "in.memory.ephemeral";

  /**
   * Set to 'true' to only forward data to Plasma. Not data storage will take place.
   */
  public static final String PURE_PLASMA = "pureplasma";

  //
  // E G R E S S
  //
  
  /**
   * Port onto which the egress server should listen
   */
  public static final String EGRESS_PORT = "egress.port";
  
  /**
   * Host onto which the egress server should listen
   */
  public static final String EGRESS_HOST = "egress.host";
  
  /**
   * Number of acceptors
   */
  public static final String EGRESS_ACCEPTORS = "egress.acceptors";
  
  /**
   * Number of selectors
   */
  public static final String EGRESS_SELECTORS = "egress.selectors";
  
  /**
   * Idle timeout
   */
  public static final String EGRESS_IDLE_TIMEOUT = "egress.idle.timeout";
  
  /**
   * ZooKeeper server list
   */
  public static final String EGRESS_ZK_QUORUM = "egress.zk.quorum";
  
  /**
   * ZooKeeper znode under which to register
   */
  public static final String EGRESS_ZK_ZNODE = "egress.zk.znode";
  
  /**
   * Key to use for encrypting GTSSplit instances
   */
  public static final String EGRESS_FETCHER_AES = "egress.fetcher.aes";
  
  /**
   * Maximum age of a valid GTSSplit (in ms)
   */
  public static final String EGRESS_FETCHER_MAXSPLITAGE = "egress.fetcher.maxsplitage";
  
  /**
   * Key to use for encrypting data in HBase (128/192/256 bits in hex or OSS reference) 
   */
  public static final String EGRESS_HBASE_DATA_AES = "egress.hbase.data.aes";
  
  /**
   * Columns family under which data should be stored
   */
  public static final String EGRESS_HBASE_DATA_COLFAM = "egress.hbase.data.colfam";
  
  /**
   * HBase table where data should be stored
   */
  public static final String EGRESS_HBASE_DATA_TABLE = "egress.hbase.data.table";
  
  /**
   * ZooKeeper Quorum for locating HBase
   */
  public static final String EGRESS_HBASE_DATA_ZKCONNECT = "egress.hbase.data.zkconnect";
  
  /**
   * Parent znode under which HBase znodes will be created
   */
  public static final String EGRESS_HBASE_DATA_ZNODE = "egress.hbase.data.znode";

  /**
   * Number of GTS to batch when retrieving datapoints (to mitigate responseTooSlow errors)
   */
  public static final String EGRESS_FETCH_BATCHSIZE = "egress.fetch.batchsize";
  
  /**
   * Boolean indicating whether or not to use the HBase filter when retrieving rows.
   */
  public static final String EGRESS_HBASE_FILTER = "egress.hbase.filter";
  
  //
  // T H R O T T L I N G    M A N A G E R
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * Name of system property (configuration property) which contains the
   * root directory where throttle files are stored.
   */  
  public static final String THROTTLING_MANAGER_DIR = "throttling.manager.dir";
  
  /**
   * Period (in ms) between two scans of the THROTTLING_MANAGER_DIR
   */
  public static final String THROTTLING_MANAGER_PERIOD = "throttling.manager.period";

  /**
   * Ramp up period (in ms) during which we do not push the estimators to Sensision.
   * This period (in ms) should be greater than the period at which the throttling files
   * are updated, so we get a chance to have a merged estimator pushed to us even when
   * we just restarted.
   */
  public static final String THROTTLING_MANAGER_RAMPUP = "throttling.manager.rampup";
  
  /**
   * Default value for the rate when not configured through a file
   */
  public static final String THROTTLING_MANAGER_RATE_DEFAULT = "throttling.manager.rate.default";

  /**
   * Default value for the mads when not configured through a file
   */
  public static final String THROTTLING_MANAGER_MADS_DEFAULT = "throttling.manager.mads.default";


  //
  // G E O D I R
  //
  /////////////////////////////////////////////////////////////////////////////////////////
  
  public static final String GEODIR_KAFKA_SUBS_ZKCONNECT = "geodir.kafka.subs.zkconnect";
  public static final String GEODIR_KAFKA_SUBS_BROKERLIST = "geodir.kafka.subs.brokerlist";
  public static final String GEODIR_KAFKA_SUBS_TOPIC = "geodir.kafka.subs.topic";
  public static final String GEODIR_KAFKA_SUBS_GROUPID = "geodir.kafka.subs.groupid";
  public static final String GEODIR_KAFKA_SUBS_NTHREADS = "geodir.kafka.subs.nthreads";
  public static final String GEODIR_KAFKA_SUBS_COMMITPERIOD = "geodir.kafka.subs.commitperiod";
  public static final String GEODIR_KAFKA_SUBS_MAC = "geodir.kafka.subs.mac";
  public static final String GEODIR_KAFKA_SUBS_AES = "geodir.kafka.subs.aes";

  public static final String GEODIR_KAFKA_DATA_ZKCONNECT = "geodir.kafka.data.zkconnect";
  public static final String GEODIR_KAFKA_DATA_BROKERLIST = "geodir.kafka.data.brokerlist";
  public static final String GEODIR_KAFKA_DATA_TOPIC = "geodir.kafka.data.topic";
  public static final String GEODIR_KAFKA_DATA_GROUPID = "geodir.kafka.data.groupid";
  public static final String GEODIR_KAFKA_DATA_NTHREADS = "geodir.kafka.data.nthreads";
  public static final String GEODIR_KAFKA_DATA_COMMITPERIOD = "geodir.kafka.data.commitperiod";
  public static final String GEODIR_KAFKA_DATA_MAC = "geodir.kafka.data.mac";
  public static final String GEODIR_KAFKA_DATA_AES = "geodir.kafka.data.aes";
  public static final String GEODIR_KAFKA_DATA_MAXSIZE = "geodir.kafka.data.maxsize";
  
  public static final String GEODIR_ID = "geodir.id";
  public static final String GEODIR_NAME = "geodir.name";
  public static final String GEODIR_MODULUS = "geodir.modulus";
  public static final String GEODIR_REMAINDER = "geodir.remainder";
  public static final String GEODIR_HTTP_PORT = "geodir.http.port";
  public static final String GEODIR_HTTP_HOST = "geodir.http.host";
  public static final String GEODIR_ACCEPTORS = "geodir.acceptors";
  public static final String GEODIR_SELECTORS = "geodir.selectors";
  public static final String GEODIR_IDLE_TIMEOUT = "geodir.idle.timeout";
  public static final String GEODIR_THRIFT_PORT = "geodir.thrift.port";
  public static final String GEODIR_THRIFT_HOST = "geodir.thrift.host";
  public static final String GEODIR_THRIFT_MAXTHREADS = "geodir.thrift.maxthreads";
  public static final String GEODIR_THRIFT_MAXFRAMELEN = "geodir.thrift.maxframelen";
  public static final String GEODIR_MAXCELLS = "geodir.maxcells";
  public static final String GEODIR_RESOLUTION = "geodir.resolution";
  public static final String GEODIR_CHUNK_DEPTH = "geodir.chunk.depth";
  public static final String GEODIR_CHUNK_COUNT = "geodir.chunk.count";
  public static final String GEODIR_PERIOD = "geodir.period";
  public static final String GEODIR_DIRECTORY_PSK = "geodir.directory.psk";
  public static final String GEODIR_FETCH_PSK = "geodir.fetch.psk";
  public static final String GEODIR_FETCH_ENDPOINT = "geodir.fetch.endpoint";
  
  public static final String GEODIR_ZK_SUBS_QUORUM = "geodir.zk.subs.quorum";
  public static final String GEODIR_ZK_SUBS_ZNODE = "geodir.zk.subs.znode";
  public static final String GEODIR_ZK_SUBS_MAXZNODESIZE = "geodir.zk.subs.maxznodesize";
  public static final String GEODIR_ZK_SUBS_AES = "geodir.zk.subs.aes";
  
  public static final String GEODIR_ZK_PLASMA_QUORUM = "geodir.zk.plasma.quorum";
  public static final String GEODIR_ZK_PLASMA_ZNODE = "geodir.zk.plasma.znode";
  public static final String GEODIR_ZK_PLASMA_MAXZNODESIZE = "geodir.zk.plasma.maxznodesize";
  
  public static final String GEODIR_ZK_SERVICE_QUORUM = "geodir.zk.service.quorum";
  public static final String GEODIR_ZK_SERVICE_ZNODE = "geodir.zk.service.znode";
  
  public static final String GEODIR_ZK_DIRECTORY_QUORUM = "geodir.zk.directory.quorum";
  public static final String GEODIR_ZK_DIRECTORY_ZNODE = "geodir.zk.directory.znode";
  
  /**
   * Comma separated list of GeoDirectory instances to maintain.
   * Each instance is defined by a string with the following format:
   * 
   * name/resolution/chunks/chunkdepth
   * 
   * name is the name of the GeoDirectory
   * resolution is a number between 1 and 15 defining the resolution of the geo index:
   * 
   * 1 = 10,000 km
   * 2 =  2,500 km
   * 3 =    625 km
   * 4 =    156 km
   * 5 =     39 km
   * 6 =     10 km
   * 7 =  2,441 m
   * 8 =    610 m
   * 9 =    153 m
   * 10=     38 m
   * 11=     10 m
   * 12=    238 cm 
   * 13=     60 cm
   * 14=     15 cm
   * 15=      4 cm
   * 
   * chunks is the number of time chunks to maintain
   * chunkdepth is the time span of each time chunk, in ms
   */
  public static final String STANDALONE_GEODIRS = "standalone.geodirs";
  
  /**
   * Delay in ms between two subscription updates
   */
  public static final String STANDALONE_GEODIR_DELAY = "standalone.geodir.delay";
  
  /**
   * Maximum number of 'cells' in the query area, system will attempt to reduce the number
   * of cells searched by replacing small cells with their enclosing parent until the number
   * of cells falls below this maximum or no more simplification can be done.
   * 
   * A good value for performance is around 256
   */
  public static final String STANDALONE_GEODIR_MAXCELLS = "standalone.geodir.maxcells";
  
  /**
   * AES encryption key for subscriptions
   */
  public static final String STANDALONE_GEODIR_AES = "standalone.geodir.aes";
  
  /**
   * Directory where subscriptions should be stored
   */
  public static final String STANDALONE_GEODIR_SUBS_DIR = "standalone.geodir.subs.dir";
  
  /**
   * Prefix for subscription files
   */
  public static final String STANDALONE_GEODIR_SUBS_PREFIX = "standalone.geodir.subs.prefix";
  
  /////////////////////////////////////////////////////////////////////////////////////////

  //
  // Jar Repository
  //
  
  public static final String JARS_DIRECTORY = "warpscript.jars.directory";
  public static final String JARS_REFRESH = "warpscript.jars.refresh";
  public static final String JARS_FROMCLASSPATH = "warpscript.jars.fromclasspath";
  
  //
  // Macro Repository
  //
  
  public static final String REPOSITORY_DIRECTORY = "warpscript.repository.directory";
  public static final String REPOSITORY_REFRESH = "warpscript.repository.refresh";

  /**
   * Header containing the request UUID when calling the endpoint
   */
  public static final String HTTP_HEADER_WEBCALL_UUIDX = "http.header.webcall.uuid";

  /**
   * HTTP Header for elapsed time of Einstein scripts
   */  
  public static final String HTTP_HEADER_ELAPSEDX = "http.header.elapsed";

  /**
   * Script line where an error was encountered
   */
  public static final String HTTP_HEADER_ERROR_LINEX = "http.header.error.line";

  /**
   * Message for the error that was encountered
   */
  public static final String HTTP_HEADER_ERROR_MESSAGEX = "http.header.error.message";

  /**
   * HTTP Header for access tokens
   */
  public static final String HTTP_HEADER_TOKENX = "http.header.token";

  /**
   * HTTP Header to provide the token for outgoing META requests
   */
  public static final String HTTP_HEADER_META_TOKENX = "http.header.token.META";

  /**
   * HTTP Header to provide the token for outgoing UPDATE requests
   */
  public static final String HTTP_HEADER_UPDATE_TOKENX = "http.header.token.UPDATE";

  /**
   * HTTP Header for access tokens used for archival
   */
  public static final String HTTP_HEADER_ARCHIVE_TOKENX = "http.header.token.archive";

  /**
   * HTTP Header for setting the base timestamp for relative timestamps
   */
  public static final String HTTP_HEADER_NOW_HEADERX = "http.header.now";
  
  /**
   * Name of header containing the signature of the token used for the fetch
   */
  public static String HTTP_HEADER_FETCH_SIGNATURE = "http.header.fetch.signature";

  /**
   * Name of header containing the signature of the token used for the update
   */
  public static String HTTP_HEADER_UPDATE_SIGNATURE = "http.header.update.signature";
  
  /**
   * Name of header containing the signature of streaming directory requests
   */
  public static String HTTP_HEADER_DIRECTORY_SIGNATURE = "http.header.directory.signature";  

  
}
