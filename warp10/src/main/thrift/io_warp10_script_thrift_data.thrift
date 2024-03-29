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
