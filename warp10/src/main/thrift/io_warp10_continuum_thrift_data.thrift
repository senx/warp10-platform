namespace java io.warp10.continuum.thrift.data

/**
 * Generic logging event
 */
struct LoggingEvent {
  /**
   * Map of attributes
   */
  1: map<string,string> attributes,
}

/**
 * Structure to serialize the parameters from an HyperLogLogPlus instance
 */
struct HyperLogLogPlusParameters {
  1: required i64 initTime,
  2: required byte p,
  3: required byte pprime,
  4: required bool sparse,
  5: optional i32 sparseListLen,
  6: optional binary sparseList,
  7: optional binary registers,
  8: optional bool gzipped,
}

/**
 * Structure to serialize GeoDirectory subscriptions for storage in ZK
 */
struct GeoDirectorySubscriptions {
  /**
   * When was this structure created
   */
  1: i64 timestamp,
  
  /**
   * Name of GeoDirectory
   */
  2: string name,
  
  /**
   * Flag indicating if we are removing the included subscriptions
   */
  3: bool removal,
  
  /**
   * Actual subscriptions per token
   */
  4: map<string,set<string>> subscriptions,
}

/**
 * Structure passed as input to the GeoDirectory service
 * It contains the set of GTS Ids among which to filter and
 * the shape to consider.
 */
struct GeoDirectoryRequest {
  /**
   * Cells of the GeoXPShape
   */
  1: list<i64> shape,
  /**
   * Flag indicating whether we search inside or outside
   * the shape
   */
  2: bool inside,
  /**
   * Map of class Id to labels Ids
   */
  3: map<i64,set<i64>> gts,
  /**
   * Start timestamp for the search (inclusive, in ms)
   */
  4: i64 startTimestamp,
  /**
   * End timestamp for the search (inclusive, in ms)
   */
  5: i64 endTimestamp,
}

struct GeoDirectoryResponse {
  /**
   * GTS Ids which match the criteria
   * of GeoDirectoryRequest
   */
  1: map<i64,set<i64>> gts,
}

/**
 * Structure used by ScriptRunner's scheduler to publish in Kafka
 * script to be run by workers.
 */
 
struct RunRequest {
  /**
   * Timestamp at which the script was scheduled
   */
  1: i64 scheduledAt,
  /**
   * Periodicity of the script
   */
  2: i64 periodicity,
  /**
   * Path to the script
   */
  3: string path,
  /**
   * Content of the script
   */
  4: binary content,
  /**
   * Flag indicating whether the content is compressed or not
   */
  5: bool compressed,
  /**
   * Id of the scheduling node
   */
  6: string scheduler,
}
