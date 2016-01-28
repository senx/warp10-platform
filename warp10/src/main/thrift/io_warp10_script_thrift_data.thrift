namespace java io.warp10.script.thrift.data

struct StackTrace {
  1: list<string> classNames,
  2: list<string> fileNames,
  3: list<string> methodNames,
  4: list<i32> lineNumbers, 
}

struct Throwable {
  1: string message,
  2: StackTrace stackTrace,
}

/**
 * Structure to log interactions with an EinsteinStack
 */
 
struct StackInteractionReport {
  /**
   * UUID of the script execution.
   */   
  1: string uuid,
  
  /**
   * Timestamp of the script execution
   */
  2: i64 timestamp,
  
  /**
   * Actual lines of the script
   */
  3: list<string> scriptLines,
  
  /**
   * Execution time of each line of the script
   */
  4: list<i64> scriptTimes,
  
  /**
   * List of all tokens used in the script
   */
  5: list<string> tokens,
}

enum WebCallMethod {
  GET = 1,
  POST = 2,
}

/**
 * Structure which holds parameters for a WebCall request
 */
struct WebCallRequest {
  /**
   * Timestamp at which the request was created.
   */
  1: i64 timestamp,
  
  /**
   * UUID of the stack on which it was issued, this is used
   * to match it with a StackInteractionReport.
   */
  2: string stackUUID,
  
  /**
   * UUID of the WebCall.
   */
  3: string webCallUUID,
  
  /**
   * Token used to issue the WebCall
   */
  4: string token,
  
  /**
   * URL to call
   */
  5: string url,
  
  /**
   * HTTP Method to use for the call
   */
  6: WebCallMethod method,
  
  /**
   * Request headers
   */
  7: map<string,string> headers,
  
  /**
   * Request body
   */
  8: string body,
}

/**
 * Structure which contains secure scripts
 */
struct SecureScript {
  /**
   * Timestamp at which the script was secured
   */
  1: i64 timestamp,
  
  /**
   * Key to unsecure the script
   */
  2: string key,
  
  /**
   * Actual script content, either a UTF8 encoded string or a compressed content
   */
  3: binary script,
  
  /**
   * Flag indicating that the script content is compressed.
   */
  4: bool compressed,
}
