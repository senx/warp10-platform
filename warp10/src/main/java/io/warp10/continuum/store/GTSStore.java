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

package io.warp10.continuum.store;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for storing/retrieving Geo Time Series
 */
public interface GTSStore {
  /**
   * Store a Geo Time Serie
   * 
   * @param token OAuth 2.0 token to use for storing data
   * @param encoder GTSEncoder instance which holds the data to store
   * @throws IOException in case an error occurs while storing
   */
  public void store(String token, GTSEncoder encoder) throws IOException;
  
  /**
   * Retrieve an AutoCloseable iterator on GeoTimeSerie instances from selection parameters.
   * 
   * @param token OAuth 2.0 token to use for accessing the data
   * @param classSelector Class selector, syntax is =EXACT_MATCH or ~REGEXP
   * @param labelsSelector Labels selector, each key is the name of a label, each value is a selector with the same syntaxt as classSelector
   * @param now Time (in us) after which no measurements will be considered.
   * @param timespan Depth of interval to consider. Any measurement older than or equal to now - timespan will be ignored
   * @return A GTSIterator on the resulting GeoTimeSerie. The returned GTS may be split in several iterator results.
   */
  public GTSIterator fetch(String token, String classSelector, Map<String,String> labelsSelector, long now, long timespan);
}
