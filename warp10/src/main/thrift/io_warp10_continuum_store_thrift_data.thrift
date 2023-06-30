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

namespace java io.warp10.continuum.store.thrift.data
include "token/src/main/thrift/io_warp10_quasar_token_thrift_data.thrift"

/**
 * Metadata describing a Geo Time Serie
 */ 
struct Metadata {
  /**
   * Name of geo time serie (class name).
   */
  1:  string name,
  
  /**
   * Labels associated with the geo time serie
   */
  2:  map<string,string> labels,
  
  /**
   * classId associated with the geo time serie
   */
  3: i64 classId,
  
  /**
   * labelsId associated with the geo time serie
   */
  4: i64 labelsId,
  
  /**
   * Attributes associated with the geo time serie.
   * Attributes are very similar to labels, except they are
   * mutable, i.e. they don't contribute to the labelsId computation.
   * They can be used to search GTS instances and store external data.
   */
  5: map<string,string> attributes,
  
  /**
   * Optional field indicating the source of the metadata. This is
   * used mainly between components to decide whether or not to
   * read/write metadata from/to persistent storage, i.e. to differentiate
   * between updates and discovery of metadata.
   */
  6: optional string source,
  
  /**
   * Timestamp (in ms since the Epoch) of the last observed activity on this Geo Time Series.
   * Activity can be update, attribute changes or deletions depending on the configuration.
   * This field is used to select GTS which have had (or not) activity after a given moment in time.
   * The last activity timestamp is an estimate of the moment of the last activity, its resolution
   * depends on the configuration of the activity window in Warp 10.
   */
  7: optional i64 lastActivity,  
}

/**
 * Types of Kafka Data Messages
 */
 
enum KafkaDataMessageType {
  STORE = 1,
  ARCHIVE = 2,
  DELETE = 3,
}

/**
 * Message passed to Kafka for data
 */
 
struct KafkaDataMessage {
  /**
   * Class Id of the data.
   */
  1: i64 classId,
  
  /**
   * Labels Id of the data.
   */
  2: i64 labelsId,
  
  /**
   * Encoded (by GTSEncoder) data for the GTS.
   */
  3: optional binary data,
  
  /**
   * List of indices specified in the update request.
   */
  4: optional list<i64> indices,
  
  /**
   * Start timestamp for deletion requests
   */
  5: optional i64 deletionStartTimestamp,
  
  /**
   * End timestamp for deletion requests
   */
  6: optional i64 deletionEndTimestamp,
  
  /**
   * Flag positioned to true if the message is a deletion
   */
  7: KafkaDataMessageType type,
  
  /**
   * Minimum age of cells to delete (in ms)
   */
  8: optional i64 deletionMinAge,

  /**
   * Optional metadata
   */
  9: optional Metadata metadata,
  
  /**
   * Message attributes, placeholder to store K/V.
   */
  10: optional map<string,string> attributes,
}

/**
 * Structure containing Geo Time Serie readings. This is mainly
 * to be used as an intermediary between GTSEncoder and a Parquet file
 */
struct GTSData {
  /**
   * Class Id of the data
   */
  1: i64 classId,
  /**
   * Labels Id of the data
   */
  2: i64 labelsId,
  /**
   * The start (oldest) timestamp in the encoded data.
   */
  3: i64 startTimestamp,
  /**
   * An optional end (youngest) timestamp if 'data' contains several values.
   * This is so we can select the record when searching for a given
   * time range.
   */
  4: optional i64 endTimestamp,
  /**
   * Encoded values of the Geo Time Serie.
   */
  5: binary data,
}

/**
 * StatsRequest
 */
struct DirectoryStatsRequest {
  /**
   * Pattern for selecting GTS class.
   */
  1: list<string> classSelector,
  
  /**
   * Patterns for selecting labels
   */
  2: list<map<string,string>> labelsSelectors,
  
  /**
   * Timestamp at which the request object was created
   */
  3: i64 timestamp,
  
  /**
   * SipHash of the request, used to ensure the requester has
   * the pre-shared key of the Directory instance.
   *
   * This hash is computed on timestamp/classSelector/labelsSelectors
   */
  4: i64 hash,
}

struct DirectoryStatsResponse {
  /**
   * Estimator for the number of distinct classes
   */
  1: binary classCardinality,
  /**
   * Estimator for the number of distinct label names
   */
  2: binary labelNamesCardinality,
  /**
   * Estimator for the number of distinct label values
   */
  3: binary labelValuesCardinality,
  /**
   * Estimations of the number of different values per label (if number of labels is small)
   */
  4: optional map<string,binary> perLabelValueCardinality,
  /**
   * Number of GTS per class (if number of different classes is small)
   */
  5: optional map<string,binary> perClassCardinality,
  /**
   * Estimation of total number of matching GTS
   */
  6: binary gtsCount,
  /**
   * Detail of encountered error
   */
  7: optional string error,
}

/**
 * Generic DirectoryRequest, container for all selection criteria.
 */
struct DirectoryRequest {
  /**
   * Patterns for selecting GTS class.
   */
  1: optional list<string> classSelectors,
  
  /**
   * Patterns for selecting labels. Each element of labelsSelectors matches the element of identical index in classSelectors
   */
  2: optional list<map<string,string>> labelsSelectors,
  
  /**
   * Timestamp (in ms) after which a given Geo Time Series was active.
   */
  3: optional i64 activeAfter,
  
  /**
   * Timestamp (in ms) after which a given Geo Time Series was quiet.
   */
  4: optional i64 quietAfter,  

  /**
   * Optional flag indicating the returned Metadata MUST absolutely be sorted.
   * This is mainly so HFDUMP can create HFiles from sorted GTS
   */
  5: optional bool sorted = false,
}

struct GTSWrapper {
  /**
   * Metadata for the GTS
   */
  1: optional Metadata metadata,
  /**
   * Last bucket for bucketized GTS
   */
  2: optional i64 lastbucket,
  /**
   * Bucket span for bucketized GTS
   */
  3: optional i64 bucketspan,
  /**
   * Bucket count for bucketized GTS
   */
  4: optional i64 bucketcount,
  /**
   * Encoding key is used
   */
  5: optional binary key,
  /**
   * Base timestamp, if not set assumed to be 0
   */
  6: optional i64 base,
  /**
   * Encoded GTS content
   */
  7: optional binary encoded,
  /**
   * Number of datapoints encoded in the wrapped GTS
   */
  8: optional i64 count,
  
  /**
   * Flag indicating if 'encoded' is gzip compressed
   */
  9: optional bool compressed = false,
  
  /**
   * Number of compression passes done on the input data
   */
  10: optional i32 compressionPasses = 1,
}

/**
 * Structure holding details of a Split
 */
struct GTSSplit {
  /**
   * When was the split created?
   */
  1: i64 timestamp,
  /**
   * Until when is the split valid?
   */
  2: i64 expiry,
  /**
   * List of metadatas associated with this split. The metadatas should contain the full set of labels so as
   * to be able to recompute class/labels Ids at the fetcher
   */
  3: list<Metadata> metadatas,
  /**
   * Read token used to generate the split
   * Having this data available allows to access the token attributes
   */
  4: optional io_warp10_quasar_token_thrift_data.ReadToken token,
}

/**
 * A MetaSet is a container for giving access to specific GTS without giving
 * away a token. It can be used to cache Directory requests or to let third
 * party access specific GTS.
 */
struct MetaSet {
  /**
   * Timestamp (in ms since epoch) at which the MetaSet expires. This is the first
   * field so it also serves as a seed for the encryption.
   */ 
  1: i64 expiry,

  /**
   * The original token used to construct the MetaSet. It will be verified when the
   * MetaSet is used.
   */
  2: string token,
    
  /**
   * List of Metadata in this MetaSet.
   */
  3: list<Metadata> metadatas,
  
  /**
   * Maximum duration this MetaSet can be used with. Setting this field
   * will force the end timestamp to be 'now' and the duration to be
   * <= maxduration if duration (timespan) is >= 0
   * >= maxduration if timespan < 0
   */
  4: optional i64 maxduration,
  
  /**
   * If this field is set, maxduration will be ignored and count based
   * requests will be forbidden.
   * The value of this field (in time units) will constraint the
   * queryable time range.
   */
  5: optional i64 notbefore,
  
  /**
   * If this field is set, maxduration will still be considered, unless
   * notbefore is also set, but the end timestamp will be constraint to
   * be <= notafter.
   */
  6: optional i64 notafter,
}

struct FetchRequest {
  /**
   * Read token to use for fetching data
   */
  1: optional io_warp10_quasar_token_thrift_data.ReadToken token,
  /**
   * List of Metadata describing the Geo Time Series to fetch
   */
  2: optional list<Metadata> metadatas,
  /**
   * End timestamp (included)
   */
  3: optional i64 now,
  /**
   * Start timestamp (included). We cannot use 'then' which is a reserved word.
   */
  4: optional i64 thents,
  /**
   * Number of data points to fetch.
   * 0 is a valid value if you want to fetch only boundaries.
   * Use -1 to specify you are not fetching by count.
   */
  5: optional i64 count = -1,
  /**
   * Number of data points to skip before starting to return values.
   */
  6: optional i64 skip = 0,
  /**
   * Index offset between two data points, defaults to 1, i.e. return every data point.
   */
  7: optional i64 step = 1,
  /**
   * Minimum time offset between data points, expressed in time units. Defaults to 1.
   */
  8: optional i64 timestep = 1,
  /**
   * Sampling rate.
   * Use 1.0D for returning all values.
   * Valid values are ( 0.0D, 1.0D ]
   */
  9: optional double sample = 1.0,
  /**
   * REMOVED - was used for HBase write timestamp retrieval
   */
  //10: optional bool writeTimestamp = false,
  /**
   * Size of the pre boundary in number of data points.
   */
  11: optional i64 preBoundary = 0,
  /**
   * Size of the post boundary in number of data points.
   */
  12: optional i64 postBoundary = 0,
  /**
   * REMOVED - was used for HBase Cell TTL retrieval
   */
  //13: optional bool TTL = false,
  /**
   * Flag indicating whether or not to use parallel scanners if available. This may be
   * needed when the order of the returned GTS is important. Performing parallel
   * scans will mix the results of all of them thus altering the order of the original
   * GTS list.
   */
  14: optional bool parallelScanners = true,
  /**
   * Map of attributes. This is used to pass information between client and backend if needed.
   */
  15: optional map<string,string> attributes,
}

enum DatalogRecordType {
  UPDATE = 1,
  DELETE = 2,
  REGISTER = 3,
  UNREGISTER = 4,
}

struct DatalogRecord {
  /**
   * Type of the Datalog Record
   */
  1: DatalogRecordType type,
  
  /**
   * Id of the instance which created the record
   */
  2: string id,
  
  /**
   * Timestamp of initial record creation, in ms since the Epoch
   */
  3: i64 timestamp,
  
  /**
   * Timestamp at which the record was added to the DatalogFile. This
   * may be different from the previous timestamp when processing a
   * DatalogRecord which was forwarded.
   */
  4: i64 storeTimestamp,
  
  /**
   * Associated Metadata
   */
  5: Metadata metadata,
  
  /**
   * Optional encode base timestamp (for UPDATE messages)
   */
  6: optional i64 baseTimestamp = 0,
  /**
   * Optional encoder (for UPDATE messages)
   */
  7: optional binary encoder,
  
  /**
   * Optional start timestamp (for DELETE messages)
   */
  8: optional i64 start = -9223372036854775808,
  /**
   * Optional end timestamp (for DELETE messages)
   */
  9: optional i64 stop = 9223372036854775807,
  
  /**
   * Optional wrapper containing the data to forward. This is a placeholder
   * where a wrapper can be stored for cases when a macro returned an extra encoder
   * which differs from the one that should be stored (contained in metadata/encoder).
   * This wrapper will be used to change the fields metadata/encoder/baseTimestamp prior
   * to storing the message in the log file. It will not be serialized.
   */
  10: optional GTSWrapper forward,
  
  /**
   * Write token associated with the request. This is needed when using FDB with tenant prefixes.
   */
  11: optional io_warp10_quasar_token_thrift_data.WriteToken token,
}

enum DatalogMessageType {
  WELCOME = 1,
  INIT = 2,
  SEEK = 3,
  TSEEK = 4,
  COMMIT = 5,
  DATA = 6,
}

struct DatalogMessage {
  1: DatalogMessageType type,
  
  //
  // Welcome related fields
  //
  // Nonce used for authentication and key derivation
  2: optional i64 nonce,
  // Timestamp used for authentication and key derivation
  3: optional i64 timestamp,
  // Flag indicating whether all further exchanges should be encrypted or not
  4: optional bool encrypt,
  
  //
  // Init related fields
  //
  11: optional string id,
  12: optional binary sig,
  13: optional list<i64> shards,
  14: optional i64 shardShift,
  15: optional list<string> excluded,
  
  //
  // Seek/Commit related field, feed will start with that ref 
  //
  // Seek ref <TS>.<UUID>:<POSITION>
  21: optional string ref,
  
  //
  // TSeek related fields, feed will start at this timestamp
  //
  // Minimum timestamp to consider
  31: optional i64 seekts,
  
  //
  // DATA messages
  //
  51: optional binary record,
  
  /**
   * Reference to use in the commit message
   */
  52: optional string commitref,
}