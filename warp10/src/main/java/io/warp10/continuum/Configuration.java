//
//   Copyright 2018-2023  SenX S.A.S.
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

import java.util.Map.Entry;
import java.util.Properties;

public class Configuration {

  //
  // FoundationDB
  //

  /**
   * API Version to use, defaults to 710
   */
  public static final String FDB_API_VERSION = "fdb.api.version";

  /**
   * If set to true then tokens MUST contain a tenant prefix and Store and Egress CANNOT specify their own tenant.
   */
  public static final String FDB_USE_TENANT_PREFIX = "fdb.use.tenant.prefix";

  public static final String OSS_MASTER_KEY = "oss.master.key";

  public static final String WARP_COMPONENTS = "warp.components";

  /**
   * Comma separated list of attributes which will invalidate the tokens they appear in
   */
  public static final String WARP_TOKEN_BANNED_ATTRIBUTES = "warp.token.banned.attributes";

  /**
   * WarpScript map defining the default attributes to allocate to read tokens. Existing attributes will not
   * be overriden if they exist
   */
  public static final String WARP_TOKEN_READ_ATTRIBUTES_DEFAULT = "warp.token.read.attributes.default";

  /**
   * WarpScript map defining the default attributes to allocate to write tokens. Existing attributes will not
   * be overriden if they exist
   */
  public static final String WARP_TOKEN_WRITE_ATTRIBUTES_DEFAULT = "warp.token.write.attributes.default";

  public static final String WARP_TOKEN_FILE = "warp.token.file";

  public static final String WARP_HASH_CLASS = "warp.hash.class";
  public static final String WARP_HASH_LABELS = "warp.hash.labels";
  public static final String WARP_HASH_TOKEN = "warp.hash.token";
  public static final String WARP_HASH_APP = "warp.hash.app";
  public static final String WARP_AES_TOKEN = "warp.aes.token";
  public static final String WARP_AES_SCRIPTS = "warp.aes.scripts";
  public static final String WARP_AES_METASETS = "warp.aes.metasets";
  public static final String WARP_AES_LOGGING = "warp.aes.logging";
  public static final String WARP_DEFAULT_AES_LOGGING = "hex:3cf5cee9eadddba796f2cce0762f308ad9df36f4883841e167dab2889bcf215b";

  /**
   * Set to 'true' to only run the analytics engine, i.e. not backend database
   */
  public static final String ANALYTICS_ENGINE_ONLY = "analytics.engine.only";

  /**
   * Prefix used for identifying keys in the configuration.
   * At startup time, any property with name warp.key.FOO will
   * be interpreted as a key, Warp will attempt to decipher the
   * key and will populate the keystore with the result under
   * name "FOO".
   */
  public static final String WARP_KEY_PREFIX = "warp.key.";

  /**
   * Prefix used for configuration keys containing secrets.
   * At startup any property warp.secret.FOO will be deciphered
   * and the result (a byte array) used as the value for key FOO.
   * This allows for storing secrets in the configuration file and
   * retrieving them via MACROCONFIG.
   */
  public static final String WARP_SECRET_PREFIX = "warp.secret.";

  public static final String WARP_IDENT = "warp.ident";

  public static final String WARP10_QUIET = "warp10.quiet";

  public static final String WARP10_TELEMETRY = "warp10.telemetry";

  /**
   * Comma separated list of headers to return in the Access-Allow-Control-Headers response header to preflight requests.
   */
  public static final String CORS_HEADERS = "cors.headers";

  /**
   * List of Warp 10 plugins to initialize
   */
  public static final String WARP10_PLUGINS = "warp10.plugins";

  /**
   * Prefix for plugin declaration
   */
  public static final String WARP10_PLUGIN_PREFIX = "warp10.plugin.";

  /**
   * Maximum number of subscriptions per plasma connection
   */
  public static final String WARP_PLASMA_MAXSUBS = "warp.plasma.maxsubs";

  /**
   * Maximum encoder size (in bytes) for internal data transfers. Use values from 64k to 512k for
   * optimum performance and make sure this size is less than the maximum message size of Kafka
   * otherwise bad things will happen as messages may not be able to be exchanged within Warp 10.
   */
  public static final String MAX_ENCODER_SIZE = "max.encoder.size";

  /**
   * WarpScript code used to resolve font URLs, can be a macro call or any other valid WarpScript excerpt
   * The code is passed the URL to check and should return the updated URL. NOOP will accept all URLs.
   */
  public static final String PROCESSING_FONT_RESOLVER = "processing.font.resolver";

  /**
   * Number of registers to allocate in stacks. Defaults to WarpScriptStack.DEFAULT_REGISTERS
   */
  public static final String CONFIG_WARPSCRIPT_REGISTERS = "warpscript.registers";

  /**
   * Maximum time to allocate for timeboxed executions
   * It can be overridden with timebox.maxtime capability
   */
  public static final String CONFIG_WARPSCRIPT_TIMEBOX_MAXTIME = "warpscript.timebox.maxtime";

  /**
   * Comma separated list of WarpScriptExtension classes to instantiate to modify
   * the defined WarpScript functions.
   */
  public static final String CONFIG_WARPSCRIPT_EXTENSIONS = "warpscript.extensions";

  /**
   * Prefix for properties which define WarpScript extensions
   */
  public static final String CONFIG_WARPSCRIPT_EXTENSION_PREFIX = "warpscript.extension.";

  /**
   * Optional WarpScript map with default capabilities
   */
  public static final String CONFIG_WARP_CAPABILITIES_DEFAULT = "warp.capabilities.default";

  /**
   * Prefix for properties which define WarpScript extension namespaces.
   */
  public static final String CONFIG_WARPSCRIPT_NAMESPACE_PREFIX = "warpscript.namespace.";

  public static final String CONFIG_WARPSCRIPT_DEFAULTCL_PREFIX = "warpscript.defaultcl.";

  /**
   * Prefix for loading a plugin using the default ClassLoader.
   * For loading plugin class foo.Bar with the default ClassLoader, add
   *
   * plugin.defaultcl.foo.Bar = true
   *
   * in the configuration
   */
  public static final String CONFIG_PLUGIN_DEFAULTCL_PREFIX = "plugin.defaultcl.";

  /**
   * This configuration parameter determines if undefining a function (via NULL 'XXX' DEF)
   * will unshadow the original statement thus making it available again or if it will replace
   * it with a function that will fail with a message saying the function is undefined.
   * The safest behavior is to leave this undefined or set to 'false'.
   */
  public static final String WARPSCRIPT_DEF_UNSHADOW = "warpscript.def.unshadow";

  public static final String WARPSCRIPT_MAX_OPS = "warpscript.maxops";
  public static final String WARPSCRIPT_MAX_BUCKETS = "warpscript.maxbuckets";
  public static final String WARPSCRIPT_MAX_GEOCELLS = "warpscript.maxgeocells";
  public static final String WARPSCRIPT_MAX_DEPTH = "warpscript.maxdepth";
  public static final String WARPSCRIPT_MAX_FETCH = "warpscript.maxfetch";
  public static final String WARPSCRIPT_MAX_GTS = "warpscript.maxgts";
  public static final String WARPSCRIPT_MAX_LOOP_DURATION = "warpscript.maxloop";
  public static final String WARPSCRIPT_MAX_RECURSION = "warpscript.maxrecursion";
  public static final String WARPSCRIPT_MAX_SYMBOLS = "warpscript.maxsymbols";
  public static final String WARPSCRIPT_MAX_PIXELS = "warpscript.maxpixels";
  public static final String WARPSCRIPT_MAX_JSON = "warpscript.maxjson";

  // Hard limits for the above limits which can be changed via a function call
  public static final String WARPSCRIPT_MAX_OPS_HARD = "warpscript.maxops.hard";
  public static final String WARPSCRIPT_MAX_BUCKETS_HARD = "warpscript.maxbuckets.hard";
  public static final String WARPSCRIPT_MAX_GEOCELLS_HARD = "warpscript.maxgeocells.hard";
  public static final String WARPSCRIPT_MAX_DEPTH_HARD = "warpscript.maxdepth.hard";
  public static final String WARPSCRIPT_MAX_FETCH_HARD = "warpscript.maxfetch.hard";
  public static final String WARPSCRIPT_MAX_GTS_HARD = "warpscript.maxgts.hard";
  public static final String WARPSCRIPT_MAX_LOOP_DURATION_HARD = "warpscript.maxloop.hard";
  public static final String WARPSCRIPT_MAX_RECURSION_HARD = "warpscript.maxrecursion.hard";
  public static final String WARPSCRIPT_MAX_SYMBOLS_HARD = "warpscript.maxsymbols.hard";
  public static final String WARPSCRIPT_MAX_PIXELS_HARD = "warpscript.maxpixels.hard";
  public static final String WARPSCRIPT_MAX_JSON_HARD = "warpscript.maxjson.hard";

  /**
   * When set to true, allow common comment block style. When false, keep the old strict comment block style within WarpScript
   */
  public static final String WARPSCRIPT_ALLOW_LOOSE_BLOCK_COMMENTS = "warpscript.comments.loose";

  /**
   * Flag to enable REXEC
   */
  public static final String WARPSCRIPT_REXEC_ENABLE = "warpscript.rexec.enable";

  /**
   * Number of continuum time units per millisecond
   * 1000000 means we store nanoseconds
   * 1000 means we store microseconds
   * 1 means we store milliseconds
   * 0.001 means we store seconds (N/A since we use a long for the constant)
   */
  public static final String WARP_TIME_UNITS = "warp.timeunits";

  /**
   * Path of the 'bootstrap' WarpScript code for Egress
   */
  public static final String CONFIG_WARPSCRIPT_BOOTSTRAP_PATH = "warpscript.bootstrap.path";

  /**
   * How often to reload the bootstrap code (in ms) for Egress
   */
  public static final String CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD = "warpscript.bootstrap.period";

  /**
   * Path of the 'bootstrap' WarpScript code for Mobius
   */
  public static final String CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PATH = "warpscript.mobius.bootstrap.path";

  /**
   * Number of threads in the Mobius pool
   */
  public static final String CONFIG_WARPSCRIPT_MOBIUS_POOL = "warpscript.mobius.pool";

  /**
   * How often to reload the bootstrap code (in ms) for Mobius
   */
  public static final String CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PERIOD = "warpscript.mobius.bootstrap.period";

  /**
   * Path of the 'bootstrap' WarpScript code for the Read Execute Loop
   */
  public static final String CONFIG_WARPSCRIPT_INTERACTIVE_BOOTSTRAP_PATH = "warpscript.interactive.bootstrap.path";

  /**
   * How often to reload the bootstrap code (in ms) for REL
   */
  public static final String CONFIG_WARPSCRIPT_INTERACTIVE_BOOTSTRAP_PERIOD = "warpscript.interactive.bootstrap.period";

  /**
   * Maximum number of parallel interactive sessions.
   */
  public static final String CONFIG_WARPSCRIPT_INTERACTIVE_CAPACITY = "warpscript.interactive.capacity";

  /**
   * Port on which the REL will listen
   */
  public static final String CONFIG_WARPSCRIPT_INTERACTIVE_TCP_PORT = "warpscript.interactive.tcp.port";

  /**
   * Path of the 'bootstrap' WarpScript code for Runner
   */
  public static final String CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PATH = "warpscript.runner.bootstrap.path";

  /**
   * How often to reload the bootstrap code (in ms) for Mobius
   */
  public static final String CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PERIOD = "warpscript.runner.bootstrap.period";

  /**
   * URL for the 'update' endpoint accessed in UPDATE
   */
  public static final String CONFIG_WARPSCRIPT_UPDATE_ENDPOINT = "warpscript.update.endpoint";

  /**
   * URL for the 'meta' endpoint accessed in META
   */
  public static final String CONFIG_WARPSCRIPT_META_ENDPOINT = "warpscript.meta.endpoint";

  /**
   * URL for the 'delete' endpoint accessed in DELETE
   */
  public static final String CONFIG_WARPSCRIPT_DELETE_ENDPOINT = "warpscript.delete.endpoint";

  /**
   * Pre-Shared key for signing fetch requests. Signed fetch request expose owner/producer
   */
  public static final String CONFIG_FETCH_PSK = "fetch.psk";

  /**
   * Maximum number of classes for which to report detailed stats in 'stats'
   */
  public static final String DIRECTORY_STATS_CLASS_MAXCARDINALITY = "directory.stats.class.maxcardinality";

  /**
   * Maximum number of labels for which to report detailed stats in 'stats'
   */
  public static final String DIRECTORY_STATS_LABELS_MAXCARDINALITY = "directory.stats.labels.maxcardinality";

  /**
   * Comma separated list of Kafka broker host:port for Kafka ('metadata' topic)
   */
  public static final String DIRECTORY_KAFKA_METADATA_CONSUMER_BOOTSTRAP_SERVERS = "directory.kafka.metadata.consumer.bootstrap.servers";

  /**
   * Prefix for Directory Kafka Metadata Consumer configuration keys
   */
  public static final String DIRECTORY_KAFKA_METADATA_CONSUMER_CONF_PREFIX = "directory.kafka.metadata.consumer.conf.prefix";

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
   * Key to use for encrypting metadata in FoundationDB (128/192/256 bits in hex or OSS reference)
   */
  public static final String DIRECTORY_FDB_METADATA_AES = "directory.fdb.metadata.aes";

  /**
   * Kafka group id with which to consume the metadata topic
   */
  public static final String DIRECTORY_KAFKA_METADATA_GROUPID = "directory.kafka.metadata.groupid";

  /**
   * Kafka client.id to use for the metadata topic consumer
   */
  public static final String DIRECTORY_KAFKA_METADATA_CONSUMER_CLIENTID = "directory.kafka.metadata.consumer.clientid";

  /**
   * Name of partition assignment strategy to use
   */
  public static final String DIRECTORY_KAFKA_METADATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "directory.kafka.metadata.consumer.partition.assignment.strategy";

  /**
   * Strategy to adopt if consuming for the first time or if the last committed offset is past Kafka history
   */
  public static final String DIRECTORY_KAFKA_METADATA_CONSUMER_AUTO_OFFSET_RESET = "directory.kafka.metadata.consumer.auto.offset.reset";

  /**
   * Delay between synchronization for offset commit
   */
  public static final String DIRECTORY_KAFKA_METADATA_COMMITPERIOD = "directory.kafka.metadata.commitperiod";

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
   * TCP Backlog applied to the DirectoryService listener
   */
  public static final String DIRECTORY_TCP_BACKLOG = "directory.tcp.backlog";

  /**
   * Port the streaming directory service listens to
   */
  public static final String DIRECTORY_STREAMING_PORT = "directory.streaming.port";

  /**
   * TCP Backlog applied to the streaming directory service listener
   */
  public static final String DIRECTORY_STREAMING_TCP_BACKLOG = "directory.streaming.tcp.backlog";

  /**
   * Should we ignore the proxy settings when doing a streaming request?
   */
  public static final String DIRECTORY_STREAMING_NOPROXY = "directory.streaming.noproxy";

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
   * Prefix used for setting Jetty attributes
   */
  public static final String DIRECTORY_STREAMING_JETTY_ATTRIBUTE_PREFIX = "directory.streaming.jetty.attribute.";

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
   * Boolean indicating whether or not we should initialized Directory by reading FoundationDB
   */
  public static final String DIRECTORY_INIT = "directory.init";

  /**
   * Boolean indicating whether or not we should store in FoundationDB metadata we get from Kafka
   */
  public static final String DIRECTORY_STORE = "directory.store";

  /**
   * Boolean indicating whether or not we should do deletions in FoundationDB
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
   * Attribute which will contain the source of the Metadata processed by the plugin
   */
  public static final String DIRECTORY_PLUGIN_SOURCEATTR = "directory.plugin.sourceattr";

  /**
   * Size of metadata cache in number of entries
   */
  public static final String DIRECTORY_METADATA_CACHE_SIZE = "directory.metadata.cache.size";

  /**
   * Activity window (in ms) to consider when deciding to store a Metadata we already know
   */
  public static final String DIRECTORY_ACTIVITY_WINDOW = "directory.activity.window";

  /**
   * Path to the FoundationDB cluster file to use for Directory data
   */
  public static final String DIRECTORY_FDB_CLUSTERFILE = "directory.fdb.clusterfile";

  /**
   * Maximum number of retries for FoundationDB transactions
   */
  public static final String DIRECTORY_FDB_RETRYLIMIT = "directory.fdb.retrylimit";

  /**
   * FoundationDB tenant to use for Directory data
   */
  public static final String DIRECTORY_FDB_TENANT = "directory.fdb.tenant";

  /**
   * Maximum size of pending mutations, going above will trigger a FoundationDB transaction commit.
   * MUST be less than the maximum FoundationDB transaction size limit (10,000,000 bytes)
   */
  public static final String DIRECTORY_FDB_METADATA_PENDINGMUTATIONS_MAXSIZE = "directory.fdb.metadata.pendingmutations.maxsize";

  //
  // I N G R E S S
  //
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Class name of ingress plugin to use
   */
  public static final String INGRESS_PLUGIN_CLASS = "ingress.plugin.class";

  /**
   * Default maximum age of datapoints pushed to Warp 10, in ms. Any timestamp older than
   * 'now' - this value will be rejected.
   * The maxpast value from the token will have precedence over this one
   */
  public static final String INGRESS_MAXPAST_DEFAULT = "ingress.maxpast.default";

  /**
   * Default maximum timestamp delta in the future for datapoints pushed to Warp 10, in ms.
   * Any timestamp more than this value past 'now' will be rejected
   * The maxfuture value from the token will have precedence over this one
   */
  public static final String INGRESS_MAXFUTURE_DEFAULT = "ingress.maxfuture.default";

  /**
   * Absolute maximum age of datapoints pushed to Warp 10, in ms. Any timestamp older than
   * 'now' - this value will be rejected.
   * This value overrides both the default and token value for maxpast.
   */
  public static final String INGRESS_MAXPAST_OVERRIDE = "ingress.maxpast.override";

  /**
   * Absolute maximum timestamp delta in the future for datapoints pushed to Warp 10, in ms.
   * Any timestamp more than this value past 'now' will be rejected
   * This value overrides both the default and token value for maxfuture
   */
  public static final String INGRESS_MAXFUTURE_OVERRIDE = "ingress.maxfuture.override";

  /**
   * Set to true to silently ignore values which are outside the allowed time range
   */
  public static final String INGRESS_OUTOFRANGE_IGNORE = "ingress.outofrange.ignore";

  /**
   * Length of the activity window in ms. If this parameter is set then GTS activity will
   * be monitored according to the configured activity events.
   */
  public static final String INGRESS_ACTIVITY_WINDOW = "ingress.activity.window";

  /**
   * Set this to true to take into account updates in the GTS activity.
   */
  public static final String INGRESS_ACTIVITY_UPDATE = "ingress.activity.update";

  /**
   * Set this to true to take into account calls to meta in the GTS activity.
   */
  public static final String INGRESS_ACTIVITY_META = "ingress.activity.meta";

  /**
   * Set to true to parse attributes in the data passed to /update.
   */
  public static final String INGRESS_PARSE_ATTRIBUTES = "ingress.parse.attributes";

  /**
   * Set to true to allow attributes to be interpreted as a delta update
   */
  public static final String INGRESS_ATTRIBUTES_ALLOWDELTA = "ingress.attributes.allowdelta";

  /**
   * Should we shuffle the GTS prior to issueing delete messages. Set to true or false.
   * It is highly recommended to set this to true as it will induce a much lower pressure
   * on region servers.
   */
  public static final String INGRESS_DELETE_SHUFFLE = "ingress.delete.shuffle";

  /**
   * If set to 'true' the /delete endpoint will reject all requests. This is useful
   * to have ingress endpoints which only honor meta and update.
   */
  public static final String INGRESS_DELETE_REJECT = "ingress.delete.reject";

  /**
   * Path where the metadata cache should be dumped
   */
  public static final String INGRESS_CACHE_DUMP_PATH = "ingress.cache.dump.path";

  /**
   * Maximum value size, make sure it is less than 'max.encoder.size'
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
   * Identification if Ingress Metadata Update endpoint source when doing a delta update of attributes
   */
  public static final String INGRESS_METADATA_UPDATE_DELTA_ENDPOINT = "ingress.metadata.update.delta";

  /**
   * Do we send Metadata in the Kafka message for delete operations?
   */
  public static final String INGRESS_DELETE_METADATA_INCLUDE = "ingress.delete.metadata.include";

  /**
   * Do we send Metadata in the Kafka message for store operations?
   */
  public static final String INGRESS_STORE_METADATA_INCLUDE = "ingress.store.metadata.include";

  /**
   * Host onto which the ingress server should listen
   */
  public static final String INGRESS_HOST = "ingress.host";

  /**
   * Port onto which the ingress server should listen
   */
  public static final String INGRESS_PORT = "ingress.port";

  /**
   * TCP Backlog applied to the ingress server listener
   */
  public static final String INGRESS_TCP_BACKLOG = "ingress.tcp.backlog";

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
   * Comma separated list of Kafka broker host:port for the metadata kafka cluster
   */
  public static final String INGRESS_KAFKA_METADATA_CONSUMER_BOOTSTRAP_SERVERS = "ingress.kafka.metadata.consumer.bootstrap.servers";

  /**
   * Prefix for Ingress Kafka Consumer configuration keys
   */
  public static final String INGRESS_KAFKA_META_CONSUMER_CONF_PREFIX = "ingress.kafka.metadata.consumer.conf.prefix";

  /**
   * Kafka broker list for the 'meta' topic
   */
  public static final String INGRESS_KAFKA_METADATA_PRODUCER_BOOTSTRAP_SERVERS = "ingress.kafka.metadata.producer.bootstrap.servers";

  /**
   * Prefix for Ingress Metadata Kafka Producer configuration keys
   */
  public static final String INGRESS_KAFKA_META_PRODUCER_CONF_PREFIX = "ingress.kafka.metadata.producer.conf.prefix";

  /**
   * Kafka client id for producing on the 'meta' topic
   */
  public static final String INGRESS_KAFKA_META_PRODUCER_CLIENTID = "ingress.kafka.metadata.producer.clientid";

  /**
   * Actual 'meta' topic
   */
  public static final String INGRESS_KAFKA_META_TOPIC = "ingress.kafka.metadata.topic";

  /**
   * Offset reset strategy.
   */
  public static final String INGRESS_KAFKA_META_CONSUMER_AUTO_OFFSET_RESET = "ingress.kafka.metadata.consumer.auto.offset.reset";

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
   * Client id to use for consuming the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_META_CONSUMER_CLIENTID = "ingress.kafka.metadata.consumer.clientid";

  /**
   * Name of partition assignment strategy to use
   */
  public static final String INGRESS_KAFKA_META_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "ingress.kafka.metadata.consumer.partition.assignment.strategy";

  /**
   * How often to commit the offsets for topic 'metadata' (in ms)
   */
  public static final String INGRESS_KAFKA_META_COMMITPERIOD = "ingress.kafka.metadata.commitperiod";

  /**
   * Number of threads to use for consuming the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_META_NTHREADS = "ingress.kafka.metadata.nthreads";

  /**
   * Kafka broker list for the 'data' topic
   */
  public static final String INGRESS_KAFKA_DATA_PRODUCER_BOOTSTRAP_SERVERS = "ingress.kafka.data.producer.bootstrap.servers";

  /**
   * Prefix for Ingress Data Kafka Producer configuration keys
   */
  public static final String INGRESS_KAFKA_DATA_PRODUCER_CONF_PREFIX = "ingress.kafka.data.producer.conf.prefix";

  /**
   * Kafka client id for producing on the 'data' topic
   */
  public static final String INGRESS_KAFKA_DATA_PRODUCER_CLIENTID = "ingress.kafka.data.producer.clientid";

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
   * Maximum message size for the 'data' topic, this value should be less than 2/3 the maximum Kafka message size
   * minus 64 to ensure all parsed data can be sent without error.
   * The maximum value size will be capped to half this value minus 64
   */
  public static final String INGRESS_KAFKA_DATA_MAXSIZE = "ingress.kafka.data.maxsize";

  /**
   * Maximum message size for the 'metadata' topic
   */
  public static final String INGRESS_KAFKA_METADATA_MAXSIZE = "ingress.kafka.metadata.maxsize";

  /**
   * Kafka broker list for the throttling topic
   */
  public static final String INGRESS_KAFKA_THROTTLING_PRODUCER_BOOTSTRAP_SERVERS = "ingress.kafka.throttling.producer.bootstrap.servers";

  /**
   * Prefix for Ingress Throttling Kafka Producer configuration keys
   */
  public static final String INGRESS_KAFKA_THROTTLING_PRODUCER_CONF_PREFIX = "ingress.kafka.throttling.producer.conf.prefix";

  /**
   * Optional client id to use when producing messages in the throttling topic
   */
  public static final String INGRESS_KAFKA_THROTTLING_PRODUCER_CLIENTID = "ingress.kafka.throttling.producer.clientid";

  /**
   * Kafka producer timeout for the throttling topic
   */
  public static final String INGRESS_KAFKA_THROTTLING_REQUEST_TIMEOUT_MS = "ingress.kafka.throttling.request.timeout.ms";

  /**
   * Name of the throttling topic
   */
  public static final String INGRESS_KAFKA_THROTTLING_TOPIC = "ingress.kafka.throttling.topic";

  /**
   * Comma separated list of Kafka broker host:port for the throttling kafka cluster
   */
  public static final String INGRESS_KAFKA_THROTTLING_CONSUMER_BOOTSTRAP_SERVERS = "ingress.kafka.throttling.consumer.bootstrap.servers";

  /**
   * Prefix for Ingress Throttling Kafka Consumer configuration keys
   */
  public static final String INGRESS_KAFKA_THROTTLING_CONSUMER_CONF_PREFIX = "ingress.kafka.throttling.consumer.conf.prefix";

  /**
   * Client id to use when consuming the throttling topic
   */
  public static final String INGRESS_KAFKA_THROTTLING_CONSUMER_CLIENTID = "ingress.kafka.throttling.consumer.clientid";

  /**
   * Group id to use when consuming the throttling topic
   */
  public static final String INGRESS_KAFKA_THROTTLING_GROUPID = "ingress.kafka.throttling.groupid";

  /**
   * Auto offset strategy to use when consuming the throttling topic. Set to 'largest' unless you want to do
   * a special experiment.
   */
  public static final String INGRESS_KAFKA_THROTTLING_CONSUMER_AUTO_OFFSET_RESET = "ingress.kafka.throttling.consumer.auto.offset.reset";

  //
  // S T O R E
  //
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Path to the FoundationDB cluster file to use for Store data
   */
  public static final String STORE_FDB_CLUSTERFILE = "store.fdb.clusterfile";

  /**
   * Maximum number of retries for FoundationDB transactions
   */
  public static final String STORE_FDB_RETRYLIMIT = "store.fdb.retrylimit";

  /**
   * FoundationDB tenant to use for Store data
   */
  public static final String STORE_FDB_TENANT = "store.fdb.tenant";

  /**
   * Path to the throttling file. This file contains a single line with a double value representing the number of mutations per second to push to FDB
   */
  public static final String STORE_THROTTLING_FILE = "store.throttling.file";

  /**
   * How often (in ms) should we read the content of the throttling file
   */
  public static final String STORE_THROTTLING_PERIOD = "store.throttling.period";

  /**
   * Key for encrypting data in FoundationDB
   */
  public static final String STORE_FDB_DATA_AES = "store.fdb.data.aes";

  /**
   * Flag indicating whether or not Store should skip writes. Used solely for benchmarking purposes.
   */
  public static final String STORE_SKIP_WRITE = "store.skip.write";

  /**
   * Comma separated list of Kafka broker host:port for Kafka ('data' topic)
   */
  public static final String STORE_KAFKA_DATA_CONSUMER_BOOTSTRAP_SERVERS = "store.kafka.data.consumer.bootstrap.servers";

  /**
   * Prefix for Store Kafka Consumer configuration keys
   */
  public static final String STORE_KAFKA_DATA_CONSUMER_CONF_PREFIX = "store.kafka.data.consumer.conf.prefix";

  /**
   * Kafka broker list for the 'data' topic
   */
  public static final String STORE_KAFKA_DATA_PRODUCER_BOOTSTRAP_SERVERS = "store.kafka.data.producer.bootstrap.servers";

  /**
   * Kafka client.id for producing on the 'data' topic
   */
  public static final String STORE_KAFKA_DATA_PRODUCER_CLIENTID = "store.kafka.data.producer.clientid";

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
   * A prefix prepended to the Kafka ConsumerId
   */
  public static final String STORE_KAFKA_DATA_CONSUMERID_PREFIX = "store.kafka.data.consumerid.prefix";

  /**
   * Client id to use to consume the data topic
   */
  public static final String STORE_KAFKA_DATA_CONSUMER_CLIENTID = "store.kafka.data.consumer.clientid";

  /**
   * Name of partition assignment strategy to use
   */
  public static final String STORE_KAFKA_DATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "store.kafka.data.consumer.partition.assignment.strategy";

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
  public static final String STORE_FDB_DATA_PENDINGMUTATIONS_MAXSIZE = "store.fdb.data.pendingmutations.maxsize";

  /**
   * How many threads (StoreConsumer) to spawn for consuming and flushing to FoundationDB
   */
  public static final String STORE_NTHREADS = "store.nthreads";

  /**
   * Number of threads for consuming Kafka in each one of the 'store.nthreads' StoreConsumer threads. Defaults to 1
   */
  public static final String STORE_NTHREADS_KAFKA = "store.nthreads.kafka";

  //
  // P L A S M A
  //
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Number of threads in Jetty's Thread Pool
   */
  public static final String PLASMA_FRONTEND_JETTY_THREADPOOL = "plasma.frontend.jetty.threadpool";

  /**
   * Maximum size of Jetty ThreadPool queue size (unbounded by default)
   */
  public static final String PLASMA_FRONTEND_JETTY_MAXQUEUESIZE = "plasma.frontend.jetty.maxqueuesize";

  /**
   * Comma separated list of Kafka broker host:port for Kafka consumer
   */
  public static final String PLASMA_FRONTEND_KAFKA_CONSUMER_BOOTSTRAP_SERVERS = "plasma.frontend.kafka.consumer.bootstrap.servers";

  /**
   * Prefix for Plasma FrontEnd Kafka Consumer configuration keys
   */
  public static final String PLASMA_FRONTEND_KAFKA_CONSUMER_CONF_PREFIX = "plasma.frontend.kafka.consumer.conf.prefix";

  /**
   * Kafka topic to consume. This topic is dedicated to this Plasma frontend.
   */
  public static final String PLASMA_FRONTEND_KAFKA_TOPIC = "plasma.frontend.kafka.topic";

  /**
   * Kafka groupid under which to consume above topic
   */
  public static final String PLASMA_FRONTEND_KAFKA_GROUPID = "plasma.frontend.kafka.groupid";

  /**
   * Kafka client id under which to consume above topic
   */
  public static final String PLASMA_FRONTEND_KAFKA_CONSUMER_CLIENTID = "plasma.frontend.kafka.consumer.clientid";

  /**
   * Name of partition assignment strategy to use
   */
  public static final String PLASMA_FRONTEND_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "plasma.frontend.kafka.consumer.partition.assignment.strategy";

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
  public static final String PLASMA_FRONTEND_ZK_QUORUM = "plasma.frontend.zk.quorum";

  /**
   * ZooKeeper root znode for subscrptions
   */
  public static final String PLASMA_FRONTEND_ZK_ZNODE = "plasma.frontend.zk.znode";

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
   * TCP Backlog applied to the Plasma listener
   */
  public static final String PLASMA_FRONTEND_TCP_BACKLOG = "plasma.frontend.tcp.backlog";

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
  public static final String PLASMA_FRONTEND_IDLE_TIMEOUT = "plasma.frontend.idle.timeout";

  /**
   * SipHash key for computing MACs of Kafka messages
   */
  public static final String PLASMA_FRONTEND_KAFKA_MAC = "plasma.frontend.kafka.mac";

  public static final String PLASMA_FRONTEND_SUBSCRIBE_DELAY = "plasma.frontend.subscribe.delay";

  /**
   * Comma separated list of Kafka broker host:port for Kafka ('in' topic)
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_CONSUMER_BOOTSTRAP_SERVERS = "plasma.backend.kafka.in.consumer.bootstrap.servers";

  /**
   * Prefix for Plasma Backend Kafka Consumer configuration keys
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_CONF_PREFIX = "plasma.backend.kafka.in.conf.prefix";

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
   * Kafka client id with which to consume the in topic
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_CONSUMER_CLIENTID = "plasma.backend.kafka.in.consumer.clientid";

  /**
   * Name of partition assignment strategy to use
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "plasma.backend.kafka.in.consumer.partition.assignment.strategy";

  /**
   * Delay between synchronization for offset commit
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_COMMITPERIOD = "plasma.backend.kafka.in.commitperiod";

  /**
   * Number of threads to run for reading off of Kafka
   */
  public static final String PLASMA_BACKEND_KAFKA_IN_NTHREADS = "plasma.backend.kafka.in.nthreads";

  /**
   * Kafka broker list for the 'out' topic
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_PRODUCER_BOOTSTRAP_SERVERS = "plasma.backend.kafka.out.producer.bootstrap.servers";

  /**
   * Prefix for Plasma BackEnd Kafka Producer configuration keys
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_PRODUCER_CONF_PREFIX = "plasma.backend.kafka.out.producer.conf.prefix";

  /**
   * Kafka client id for producing on the 'out' topic
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_PRODUCER_CLIENTID = "plasma.backend.kafka.out.producer.clientid";

  /**
   * Maximum size of Kafka outward messages
   */
  public static final String PLASMA_BACKEND_KAFKA_OUT_MAXSIZE = "plasma.backend.kafka.out.maxsize";

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
  public static final String PLASMA_BACKEND_SUBSCRIPTIONS_ZK_QUORUM = "plasma.backend.subscriptions.zk.quorum";

  /**
   * Parent znode under which subscription znodes will be created
   */
  public static final String PLASMA_BACKEND_SUBSCRIPTIONS_ZK_ZNODE = "plasma.backend.subscriptions.zk.znode";


  //
  // R U N N E R
  //
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Boolean indicating whether the first run of each script should be at startup (the default behavior) or
   * at the next round scheduling period.
   */
  public static final String RUNNER_RUNATSTARTUP = "runner.runatstartup";

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
   * WarpScript endpoint to use for executing the scripts
   */
  public static final String RUNNER_ENDPOINT = "runner.endpoint";

  /**
   * Minimum period at which a script can be scheduled. Any script scheduled
   * more often than that won't be run
   */
  public static final String RUNNER_MINPERIOD = "runner.minperiod";

  /**
   * Comma separated list of Kafka broker host:port for the Kafka cluster
   */
  public static final String RUNNER_KAFKA_CONSUMER_BOOTSTRAP_SERVERS = "runner.kafka.consumer.bootstrap.servers";

  /**
   * Prefix for Runner Kafka Consumer configuration keys
   */
  public static final String RUNNER_KAFKA_CONSUMER_CONF_PREFIX = "runner.kafka.consumer.conf.prefix";

  /**
   * List of Kafka brokers
   */
  public static final String RUNNER_KAFKA_PRODUCER_BOOTSTRAP_SERVERS = "runner.kafka.producer.bootstrap.servers";

  /**
   * Prefix for Runner Kafka Producer configuration keys
   */
  public static final String RUNNER_KAFKA_PRODUCER_CONF_PREFIX = "runner.kafka.producer.conf.prefix";

  /**
   * Kafka client id for producing on the runner topic
   */
  public static final String RUNNER_KAFKA_PRODUCER_CLIENTID = "runner.kafka.producer.clientid";

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
   * Client id to use when consuming scripts
   */
  public static final String RUNNER_KAFKA_CONSUMER_CLIENTID = "runner.kafka.consumer.clientid";

  /**
   * Name of partition assignment strategy to use
   */
  public static final String RUNNER_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = "runner.kafka.consumer.partition.assignment.strategy";

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

  /**
   * PreShared key for identifying scripts executing from runner
   */
  public static final String RUNNER_PSK = "runner.psk";

  //
  // S T A N D A L O N E
  //
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Number of threads in Jetty's Thread Pool
   */
  public static final String STANDALONE_JETTY_THREADPOOL = "standalone.jetty.threadpool";

  /**
   * Maximum size of Jetty ThreadPool queue size (unbounded by default)
   */
  public static final String STANDALONE_JETTY_MAXQUEUESIZE = "standalone.jetty.maxqueuesize";

  /**
   * Geo Time Series count above which block caching will be disabled for LevelDB.
   * The goal is to limit the cache pollution when scanning large chunks of data.
   * Note that this limit is per fetch call to the backend, which means that in the case of parallel scanners it is for each parallel fetch attempt.
   */
  public static final String LEVELDB_BLOCKCACHE_GTS_THRESHOLD = "leveldb.blockcache.gts.threshold";

  /**
   * Rate of synchronous writes for the datapoints (update/deletes).
   * This is a double between 0.0 (all writes asynchronous) and 1.0 (all writes synchronous).
   * The default value is 1.0 (all writes are synchronous)
   */
  public static final String LEVELDB_DATA_SYNCRATE = "leveldb.data.syncrate";

  /**
   * Maximum number of SST files that can be removed during a single call to SSTPURGE
   */
  public static final String LEVELDB_MAXPURGE_KEY = "leveldb.maxpurge";

  /**
   * Rate of synchronous writes for the directory writes.
   * This is a double between 0.0 (all writes asynchronous) and 1.0 (all writes synchronous)
   * The default value is 1.0 (all writes are synchronous)
   */
  public static final String LEVELDB_DIRECTORY_SYNCRATE = "leveldb.directory.syncrate";

  /**
   * Flag to disable the use of the native LevelDB implementation
   */
  public static final String LEVELDB_NATIVE_DISABLE = "leveldb.native.disable";

  /**
   * Flag to disable the use of the pure java LevelDB implementation
   */
  public static final String LEVELDB_JAVA_DISABLE = "leveldb.java.disable";

  /**
   * Directory where the leveldb files should be created
   */
  public static final String LEVELDB_HOME = "leveldb.home";

  /**
   * Create LevelDB if not already initialized
   */
  public static final String LEVELDB_CREATE_IF_MISSING = "leveldb.create.if.missing";

  /**
   * Maximum number of open files to use for LevelDB
   */
  public static final String LEVELDB_MAXOPENFILES = "leveldb.maxopenfiles";

  /**
   * AES key to use for wrapping metadata prior to storage in leveldb
   */
  public static final String LEVELDB_METADATA_AES = "leveldb.metadata.aes";

  /**
   * AES key to use for wrapping datapoints prior to storage in leveldb
   */
  public static final String LEVELDB_DATA_AES = "leveldb.data.aes";

  /**
   * Cache size for leveldb (in bytes)
   */
  public static final String LEVELDB_CACHE_SIZE = "leveldb.cache.size";

  /**
   * LevelDB block size to use
   */
  public static final String LEVELDB_BLOCK_SIZE = "leveldb.block.size";

  /**
   * LevelDB block restart interval
   */
  public static final String LEVELDB_BLOCK_RESTART_INTERVAL = "leveldb.block.restart.interval";

  /**
   * Flag indicating whether or not to verify block checksums
   */
  public static final String LEVELDB_VERIFY_CHECKSUMS = "leveldb.verify.checksums";

  /**
   * Flag indicating whether or not to perform paranoid checks
   */
  public static final String LEVELDB_PARANOID_CHECKS = "leveldb.paranoid.checks";

  /**
   * Compression type to use for leveldb (SNAPPY/NONE)
   */
  public static final String LEVELDB_COMPRESSION_TYPE = "leveldb.compression.type";

  /**
   * Size of write buffer (in bytes) - defaults to 4194304
   */
  public static final String LEVELDB_WRITEBUFFER_SIZE = "leveldb.writebuffer.size";

  /**
   * Flag indicating whether or not we tolerate pre-existing LevelDB directory
   */
  public static final String LEVELDB_ERROR_IF_EXISTS = "leveldb.error.if.exists";

  /**
   * Set to true to disable the delete endpoint in the standalone version of Warp 10.
   */
  public static final String STANDALONE_DELETE_DISABLE = "standalone.delete.disable";

  /**
   * Set to true to enable splits generation on the standalone instance. This MUST be set
   * to true for Warp10InputFormat to work against a standalone Warp 10 instance.
   */
  public static final String STANDALONE_SPLITS_ENABLE = "standalone.splits.enable";

  /**
   * IP to bind to for listening to incoming connections. Use 0.0.0.0 to listen to all interfaces
   */
  public static final String STANDALONE_HOST = "standalone.host";

  /**
   * Port to bind to for listening to incoming connections.
   */
  public static final String STANDALONE_PORT = "standalone.port";

  /**
   * TCP Backlog applied to incoming connections listener.
   */
  public static final String STANDALONE_TCP_BACKLOG = "standalone.tcp.backlog";

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
   * Maximum number of keys to batch when deleting data
   */
  public static final String STANDALONE_MAX_DELETE_BATCHSIZE = "standalone.max.delete.batchsize";

  /**
   * Should the LevelDB cache be filled when scanning key space for deletes? Defaults to false.
   */
  public static final String LEVELDB_DELETE_FILLCACHE = "leveldb.delete.fillcache";

  /**
   * Should LevelDB checksums be verified when scanning key space for deletes? Defaults to true.
   */
  public static final String LEVELDB_DELETE_VERIFYCHECKSUMS = "leveldb.delete.verifychecksums";

  /**
   * Maximum size in bytes of a value
   */
  public static final String STANDALONE_VALUE_MAXSIZE = "standalone.value.maxsize";

  /**
   * Comma separated list of shards this Warp 10 instance should store. The format of each
   * shard is MODULUS:REMAINDER
   */
  public static final String DATALOG_SHARDS = "datalog.shards";

  /**
   * Number of bits to shift the default shard key
   */
  public static final String DATALOG_SHARDKEY_SHIFT = "datalog.shardkey.shift";

  /**
   * Datalog Manager Fully Qualified Class Name
   */
  public static final String DATALOG_MANAGER = "datalog.manager";

  /**
   * Comma separated list of consumer names
   */
  public static final String DATALOG_CONSUMERS = "datalog.consumers";

  /**
   * Class name of consumer.
   */
  public static final String DATALOG_CONSUMER_CLASS_PREFIX = "datalog.consumer.class.";

  /**
   * Flag indicating whether register events should be transmitted systematically (true)
   * or simply when the local Directory actually stored the Metadata (false, the default).
   * This will prevent architectures with multiple endpoints to miss Metadata creations when
   * the feeds lag behind.
   */
  public static final String DATALOG_REGISTER_ALL = "datalog.register.all";

  /**
   * Maximum length of class names - Defaults to 1024
   */
  public static final String WARP_CLASS_MAXSIZE = "warp.class.maxsize";

  /**
   * Maximum length of labels (names + values) - Defaults to 2048
   */
  public static final String WARP_LABELS_MAXSIZE = "warp.labels.maxsize";

  /**
   * Default priority order for matching labels when doing a FIND/FETCH.
   * Comma separated list of label names.
   * Defaults to .producer,.app,.owner
   */
  public static final String WARPSCRIPT_LABELS_PRIORITY = "warpscript.labels.priority";

  /**
   * Maximum length of attributes (names + values) - Defaults to 8192
   */
  public static final String WARP_ATTRIBUTES_MAXSIZE = "warp.attributes.maxsize";

  /**
   * Set to a message indicating the reason why updates are disabled, they are enabled if this is not set
   */
  public static final String WARP_UPDATE_DISABLED = "warp.update.disabled";

  /**
   * Set to true to expose owner and producer labels in Geo Time Series retrieved from the Warp 10 Storage Engine
   */
  public static final String WARP10_EXPOSE_OWNER_PRODUCER = "warp10.expose.owner.producer";

  /**
   * Set to true to allow Directory queries with missing label selectors (using empty exact match)
   */
  public static final String WARP10_ABSENT_LABEL_SUPPORT = "warp10.absent.label.support";

  /**
   * Set to true to allow the /delete endpoint to only delete metadata.
   */
  public static final String INGRESS_DELETE_METAONLY_SUPPORT = "ingress.delete.metaonly.support";

  /**
   * Set to true to allow activeafter/quietafter parameters to delete requests.
   * This must be explicitely configured to avoid deleting extraneous GTS when using those parameters when no
   * activity tracking is active.
   */
  public static final String INGRESS_DELETE_ACTIVITY_SUPPORT = "ingress.delete.activity.support";

  /**
   * Set to a message indicating the reason why deletes are disabled, they are enabled if this is not set
   */
  public static final String WARP_DELETE_DISABLED = "warp.delete.disabled";

  /**
   * Set to a message indicating the reason why meta updates are disabled, they are enabled if this is not set
   */
  public static final String WARP_META_DISABLED = "warp.meta.disabled";

  /**
   * Set to 'true' to disable plasma
   */
  public static final String WARP_PLASMA_DISABLE = "warp.plasma.disable";

  /**
   * Set to 'true' to disable mobius
   */
  public static final String WARP_MOBIUS_DISABLE = "warp.mobius.disable";

  /**
   * Set to 'true' to disable the Read Execute Loop
   */
  public static final String WARP_INTERACTIVE_DISABLE = "warp.interactive.disable";

  /**
   * Set to 'true' to disable stream updates
   */
  public static final String WARP_STREAMUPDATE_DISABLE = "warp.streamupdate.disable";

  /**
   * Storage backend to use.
   */
  public static final String BACKEND = "backend";

  /**
   * Set to 'true' to have an in-memory cache ahead of the persistent store.
   * in.memory.chunk.count and in.memory.chunk.length MUST be defined
   */
  public static final String ACCELERATOR = "accelerator";

  /**
   * Default accelerator strategy for writes.
   * Can contain 'cache', 'nocache', 'persist' and 'nopersist'.
   * Defaults to 'cache persist'.
   */
  public static final String ACCELERATOR_DEFAULT_WRITE = "accelerator.default.write";

  /**
   * Default accelerator strategy for reads.
   * Can contain 'cache', 'nocache', 'persist' and 'nopersist'.
   * Defaults to 'cache persist'.
   */
  public static final String ACCELERATOR_DEFAULT_READ = "accelerator.default.read";

  /**
   * Default accelerator strategy for deletes.
   * Can contain 'cache', 'nocache', 'persist' and 'nopersist'.
   * Defaults to 'cache persist'.
   */
  public static final String ACCELERATOR_DEFAULT_DELETE = "accelerator.default.delete";

  /**
   * Set to 'true' to preload the accelerator with the persisted data spanning the accelerator time range.
   * Preloading can be disabled for setups where the accelerator is used as a temporary side cache only.
   */
  public static final String ACCELERATOR_PRELOAD = "accelerator.preload";

  /**
   * Set to 'true' to preload the accelerator with data based on the lastactivity
   */
  public static final String ACCELERATOR_PRELOAD_ACTIVITY = "accelerator.preload.activity";

  /**
   * Number of threads to use for preloading the accelerator
   */
  public static final String ACCELERATOR_PRELOAD_POOLSIZE = "accelerator.preload.poolsize";

  /**
   * Batch size to use for preloading the accelerator
   */
  public static final String ACCELERATOR_PRELOAD_BATCHSIZE = "accelerator.preload.batchsize";

  /**
   * Number of chunks per GTS to handle in memory
   */
  public static final String ACCELERATOR_CHUNK_COUNT = "accelerator.chunk.count";

  /**
   * Length of each chunk (in time units)
   */
  public static final String ACCELERATOR_CHUNK_LENGTH = "accelerator.chunk.length";

  /**
   * If set to true, then only the last recorded value of a GTS is kept
   */
  public static final String ACCELERATOR_EPHEMERAL = "accelerator.ephemeral";

  /**
   * How often (in ms) to perform a gc of the Warp 10 accelerator.
   */
  public static final String ACCELERATOR_GC_PERIOD = "accelerator.gcperiod";

  /**
   * Maximum size (in bytes) of re-allocations performed during a gc cycle of the Warp 10 accelerator
   */
  public static final String ACCELERATOR_GC_MAXALLOC = "accelerator.gc.maxalloc";

  /**
   * If set to true, then only the last recorded value of a GTS is kept
   */
  public static final String IN_MEMORY_EPHEMERAL = "in.memory.ephemeral";

  /**
   * Number of chunks per GTS to handle in memory (defaults to 3)
   */
  public static final String IN_MEMORY_CHUNK_COUNT = "in.memory.chunk.count";

  /**
   * Length of each chunk (in time units), defaults to Long.MAX_VALUE
   */
  public static final String IN_MEMORY_CHUNK_LENGTH = "in.memory.chunk.length";

  /**
   * Path to a dump file containing the state of an in-memory Warp 10 to restore.
   */
  public static final String STANDALONE_MEMORY_STORE_LOAD = "in.memory.load";

  /**
   * Path to a dump file in which the current state of an in-memory Warp 10 will be persisted.
   */
  public static final String STANDALONE_MEMORY_STORE_DUMP = "in.memory.dump";

  /**
   * Set to true to tolerate errors while loading a dumped state. Setting this to true can lead to partial data being loaded.
   */
  public static final String STANDALONE_MEMORY_STORE_LOAD_FAILSAFE = "in.memory.load.failsafe";

  /**
   * How often (in ms) to perform a gc of the in-memory store.
   */
  public static final String STANDALONE_MEMORY_GC_PERIOD = "in.memory.gcperiod";

  /**
   * Maximum size (in bytes) of re-allocations performed during a gc cycle of the chunked in-memory store.
   */
  public static final String STANDALONE_MEMORY_GC_MAXALLOC = "in.memory.gc.maxalloc";

  //
  // E G R E S S
  //

  /**
   * Path to the FoundationDB cluster file to use for Egress data
   */
  public static final String EGRESS_FDB_CLUSTERFILE = "egress.fdb.clusterfile";

  /**
   * FoundationDB tenant to use for Egress data
   */
  public static final String EGRESS_FDB_TENANT = "egress.fdb.tenant";

  /**
   * Size of pooled FoundationDB databases instances
   */
  public static final String EGRESS_FDB_POOLSIZE = "egress.fdb.poolsize";

  /**
   * Maximum allowed execution time per script execution (in ms). Can be modified
   * via the token attribute .maxtime
   */
  public static final String EGRESS_MAXTIME = "egress.maxtime";

  /**
   * Validity (in ms) of a runner nonce. This is used to determine if timeboxing should be waived or not for a runner call.
   */
  public static final String EGRESS_RUNNER_NONCE_VALIDITY = "egress.runner.nonce.validity";

  /**
   * Number of threads in Jetty's Thread Pool
   */
  public static final String EGRESS_JETTY_THREADPOOL = "egress.jetty.threadpool";

  /**
   * Maximum size of Jetty ThreadPool queue size (unbounded by default)
   */
  public static final String EGRESS_JETTY_MAXQUEUESIZE = "egress.jetty.maxqueuesize";


  /**
   * Flag (true/false) indicating whether or not the Directory and Store clients should be exposed by Egress.
   * If set to true then Warp 10 plugins might access the exposed clients via the getExposedDirectoryClient and
   * getExposedStoreClient static methods of EgressExecHandler.
   */
  public static final String EGRESS_CLIENTS_EXPOSE = "egress.clients.expose";

  /**
   * Port onto which the egress server should listen
   */
  public static final String EGRESS_PORT = "egress.port";

  /**
   * TCP Backlog applied to the egress server listener
   */
  public static final String EGRESS_TCP_BACKLOG = "egress.tcp.backlog";

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
   * Key to use for encrypting GTSSplit instances
   */
  public static final String EGRESS_FETCHER_AES = "egress.fetcher.aes";

  /**
   * Maximum age of a valid GTSSplit (in ms)
   */
  public static final String EGRESS_FETCHER_MAXSPLITAGE = "egress.fetcher.maxsplitage";

  /**
   * Number of threads to use for scheduling parallel scanners. Use 0 to disable parallel scanners
   */
  public static final String EGRESS_FDB_PARALLELSCANNERS_POOLSIZE = "egress.fdb.parallelscanners.poolsize";

  /**
   * Maximum number of parallel scanners per fetch request. Use 0 to disable parallel scanners.
   */
  public static final String EGRESS_FDB_PARALLELSCANNERS_MAXINFLIGHTPERREQUEST = "egress.fdb.parallelscanners.maxinflightperrequest";

  /**
   * Minimum number of GTS to assign to a parallel scanner. If the number of GTS to fetch is below this limit, no
   * parallel scanners will be spawned. Defaults to 4.
   */
  public static final String EGRESS_FDB_PARALLELSCANNERS_MIN_GTS_PERSCANNER = "egress.fdb.parallelscanners.min.gts.perscanner";

  /**
   * Maximum number of parallel scanners to use when fetching datapoints for a batch of GTS (see EGRESS_FETCH_BATCHSIZE).
   * Defaults to 16.
   */
  public static final String EGRESS_FDB_PARALLELSCANNERS_MAX_PARALLEL_SCANNERS = "egress.fdb.parallelscanners.max.parallel.scanners";

  /**
   * Number of threads to use for scheduling parallel scanners in the standalone version. Use 0 to disable parallel scanners
   */
  public static final String STANDALONE_PARALLELSCANNERS_POOLSIZE = "standalone.parallelscanners.poolsize";

  /**
   * Maximum number of parallel scanners per fetch request in the standalone version. Use 0 to disable parallel scanners.
   */
  public static final String STANDALONE_PARALLELSCANNERS_MAXINFLIGHTPERREQUEST = "standalone.parallelscanners.maxinflightperrequest";

  /**
   * Minimum number of GTS to assign to a parallel scanner in the standalone version. If the number of GTS to fetch is below this limit, no
   * parallel scanners will be spawned. Defaults to 4.
   */
  public static final String STANDALONE_PARALLELSCANNERS_MIN_GTS_PERSCANNER = "standalone.parallelscanners.min.gts.perscanner";

  /**
   * Maximum number of parallel scanners to use when fetching datapoints for a batch of GTS (see EGRESS_FETCH_BATCHSIZE) in the standalone version.
   * Defaults to 16.
   */
  public static final String STANDALONE_PARALLELSCANNERS_MAX_PARALLEL_SCANNERS = "standalone.parallelscanners.max.parallel.scanners";

  /**
   * Key to use for encrypting data in FDB (128/192/256 bits in hex or OSS reference)
   */
  public static final String EGRESS_FDB_DATA_AES = "egress.fdb.data.aes";

  /**
   * Number of GTS to batch when retrieving datapoints (to mitigate responseTooSlow errors)
   */
  public static final String EGRESS_FETCH_BATCHSIZE = "egress.fetch.batchsize";

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
   * Maximum number of estimators we keep in memory
   */
  public static final String THROTTLING_MANAGER_ESTIMATOR_CACHE_SIZE = "throttling.manager.estimator.cache.size";

  /**
   * Default value for the rate when not configured through a file
   */
  public static final String THROTTLING_MANAGER_RATE_DEFAULT = "throttling.manager.rate.default";

  /**
   * Default value for the mads when not configured through a file
   */
  public static final String THROTTLING_MANAGER_MADS_DEFAULT = "throttling.manager.mads.default";

  /**
   * Default value for the maxwait timeout
   */
  public static final String THROTTLING_MANAGER_MAXWAIT_DEFAULT = "throttling.manager.maxwait.default";

  /**
   * Size of macro cache for the macros loaded from the classpath
   */
  public static final String WARPSCRIPT_LIBRARY_CACHE_SIZE = "warpscript.library.cache.size";

  /**
   * Default TTL for macros loaded from the classpath
   */
  public static final String WARPSCRIPT_LIBRARY_TTL = "warpscript.library.ttl";

  /**
   * Maximum TTL for a macro loaded from the classpath
   */
  public static final String WARPSCRIPT_LIBRARY_TTL_HARD = "warpscript.library.ttl.hard";

  /*
   * CALL root directory property
   */

  public static final String WARPSCRIPT_CALL_DIRECTORY = "warpscript.call.directory";

  /**
   * Maximum number of subprogram instances which can be spawned
   */
  public static final String WARPSCRIPT_CALL_MAXCAPACITY = "warpscript.call.maxcapacity";

  /**
   * Maximum amount of time each attempt to access a process will wait (in ms). Defaults to 10000 ms.
   */
  public static final String WARPSCRIPT_CALL_MAXWAIT = "warpscript.call.maxwait";

  /**
   * Macro Repository root directory
   */
  public static final String REPOSITORY_DIRECTORY = "warpscript.repository.directory";

  /**
   * Number of macros loaded from 'warpscript.repository.directory' to keep in memory
   */
  public static final String REPOSITORY_CACHE_SIZE = "warpscript.repository.cache.size";

  /**
   * Macro repository refresh interval (in ms).
   * If set to 0 then ondemand will be forced to true and no automatic loading of macros will occur.
   */
  public static final String REPOSITORY_REFRESH = "warpscript.repository.refresh";

  /**
   * Default TTL for macros loaded on demand
   */
  public static final String REPOSITORY_TTL = "warpscript.repository.ttl";

  /**
   * Max TTL for macros loaded on demand (will limit the value one can set using MACROTTL)
   */
  public static final String REPOSITORY_TTL_HARD = "warpscript.repository.ttl.hard";

  /**
   * TTL to use for failed macros
   */
  public static final String REPOSITORY_TTL_FAILED = "warpscript.repository.ttl.failed";

  /**
   * Should new macros be loaded on demand?
   */
  public static final String REPOSITORY_ONDEMAND = "warpscript.repository.ondemand";

  /**
   * Comma separated list of configured WarpFleet™ repositories
   */
  public static final String WARPFLEET_MACROS_REPOS = "warpfleet.macros.repos";

  /**
   * Default value for warpfleet.macros.repos if it is not set
   */
  public static final String WARPFLEET_MACROS_REPOS_DEFAULT = "https://warpfleet.senx.io/macros";

  /**
   * Maximum number of cached macros in the cache
   */
  public static final String WARPFLEET_CACHE_SIZE = "warpfleet.cache.size";

  /**
   * Default TTL (in ms) for macros loaded from a WarpFleet repository
   */
  public static final String WARPFLEET_MACROS_TTL = "warpfleet.macros.ttl";

  /**
   * Lower limit for TTL (in ms) of macros loaded from a WarpFleet repository
   */
  public static final String WARPFLEET_MACROS_TTL_MIN = "warpfleet.macros.ttl.min";

  /**
   * Upper limit for TTL (in ms) of macros loaded from a WarpFleet repository
   */
  public static final String WARPFLEET_MACROS_TTL_MAX = "warpfleet.macros.ttl.max";

  /**
   * Default TTL (in ms) for WarpFleet macros which had errors
   */
  public static final String WARPFLEET_MACROS_TTL_FAILED = "warpfleet.macros.ttl.failed";

  /**
   * Default TTL (in ms) for WarpFleet macros which were not found. If > 0, a dummy macro
   * will be generated which will fail with an informative error message
   */
  public static final String WARPFLEET_MACROS_TTL_UNKNOWN = "warpfleet.macros.ttl.unknown";

  /**
   * Read timeout when fetching macro source code from a repository, in ms. Defaults to 10s.
   */
  public static final String WARPFLEET_TIMEOUT_READ = "warpfleet.timeout.read";

  /**
   * Connection timeout when fetching macro source code from a repository, in ms. Defaults to 5s.
   */
  public static final String WARPFLEET_TIMEOUT_CONNECT = "warpfleet.timeout.connect";

  /**
   * Name of WarpFleet repository macro. This macro consumes a URL and emits a boolean.
   */
  public static final String WARPFLEET_MACROS_VALIDATOR = "warpfleet.macros.validator";

  /**
   * Configuration key to modify the capabilities header
   */
  public static final String HTTP_HEADER_CAPABILITIES = "http.header.capabilities";

  /**
   * HTTP Header for elapsed time of WarpScript scripts
   */
  public static final String HTTP_HEADER_ELAPSEDX = "http.header.elapsed";

  /**
   * HTTP Header for number of ops performed in a script invocation
   */
  public static final String HTTP_HEADER_OPSX = "http.header.ops";

  /**
   * HTTP Header for number of datapoints fetched during a script invocation
   */
  public static final String HTTP_HEADER_FETCHEDX = "http.header.fetched";

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
   * HTTP Header to provide the token for outgoing DELETE requests
   */
  public static final String HTTP_HEADER_DELETE_TOKENX = "http.header.token.DELETE";

  /**
   * HTTP Header to provide the token for outgoing UPDATE requests
   */
  public static final String HTTP_HEADER_UPDATE_TOKENX = "http.header.token.UPDATE";

  /**
   * HTTP Header for setting the base timestamp for relative timestamps or for the 'now'
   * parameter of /sfetch
   */
  public static final String HTTP_HEADER_NOW_HEADERX = "http.header.now";

  /**
   * HTTP Header for specifying attribute parsing type
   */
  public static final String HTTP_HEADER_ATTRIBUTES = "http.header.attributes";

  /**
   * HTTP Header for specifying the timespan in /sfetch requests
   */
  public static final String HTTP_HEADER_TIMESPAN_HEADERX = "http.header.timespan";

  /**
   * HTTP Header to specify if we should show errors in /sfetch responses
   */
  public static final String HTTP_HEADER_SHOW_ERRORS_HEADERX = "http.header.showerrors";

  /**
   * Name of header containing the signature of the token used for the fetch
   */
  public static final String HTTP_HEADER_FETCH_SIGNATURE = "http.header.fetch.signature";

  /**
   * Name of header containing the signature of the token used for the update
   */
  public static final String HTTP_HEADER_UPDATE_SIGNATURE = "http.header.update.signature";

  /**
   * Name of header containing the signature of streaming directory requests
   */
  public static final String HTTP_HEADER_DIRECTORY_SIGNATURE = "http.header.directory.signature";

  /**
   * Name of header containing the name of the symbol in which to expose the request headers
   */
  public static final String HTTP_HEADER_EXPOSE_HEADERS = "http.header.exposeheaders";

  /**
   * SSL Port
   */
  public static final String _SSL_PORT = ".ssl.port";

  /**
   * SSL TCP Backlog
   */
  public static final String _SSL_TCP_BACKLOG = ".ssl.tcp.backlog";

  /**
   * SSL Host
   */
  public static final String _SSL_HOST = ".ssl.host";

  /**
   * SSL Acceptors
   */
  public static final String _SSL_ACCEPTORS = ".ssl.acceptors";

  /**
   * SSL Selectors
   */
  public static final String _SSL_SELECTORS = ".ssl.selectors";

  /**
   * SSL KeyStore path
   */
  public static final String _SSL_KEYSTORE_PATH = ".ssl.keystore.path";

  /**
   * SSL KeyStore password
   */
  public static final String _SSL_KEYSTORE_PASSWORD = ".ssl.keystore.password";

  /**
   * Alias associated with the certificate to use
   */
  public static final String _SSL_CERT_ALIAS = ".ssl.cert.alias";

  /**
   * SSL KeyManager password
   */
  public static final String _SSL_KEYMANAGER_PASSWORD = ".ssl.keymanager.password";

  /**
   * SSL Idle timeout
   */
  public static final String _SSL_IDLE_TIMEOUT = ".ssl.idle.timeout";

  //
  // Prefixes for the SSL configs
  //

  /**
   * Extract properties which have a given prefix an return a Properties instance
   * with those properties from which the prefix was removed.
   *
   * @param properties Properties to inspect
   * @param prefix Prefix to detect
   * @return the subset of properties which had the given prefix (removed)
   */
  public static Properties extractPrefixed(Properties properties, String prefix) {
    Properties extract = new Properties();

    if (null != prefix) {
      for (Entry<Object, Object> entry: properties.entrySet()) {
        String key = entry.getKey().toString();
        if (key.startsWith(prefix)) {
          extract.put(key.substring(prefix.length()), entry.getValue());
        }
      }
    }

    return extract;
  }

  public static final String STANDALONE_PREFIX = "standalone";
  public static final String EGRESS_PREFIX = "egress";
  public static final String INGRESS_PREFIX = "ingress";
  public static final String PLASMA_FRONTEND_PREFIX = "plasma.frontend";

  //
  // Hadoop Integration Configurations
  //

  /**
   * Set to 'true' to throw an error when a Writable that the WritableUtils cannot convert is encountered.
   * If this is not set, the unknown Writable will be returned as is.
   */
  public static final String CONFIG_WARPSCRIPT_HADOOP_STRICTWRITABLES = "warpscript.hadoop.strictwritables";

  /**
   * Set to 'true' to return Writable instances as is in WarpScriptInputFormat
   */
  public static final String CONFIG_WARPSCRIPT_HADOOP_RAWWRITABLES = "warpscript.hadoop.rawwritables";

  /**
   * Flag (true/false) indicating whether or not to use capabilities for controlling access to debug functions. Defaults to true.
   */
  public static final String CONFIG_DEBUG_CAPABILITY = "debug.capability";
}
