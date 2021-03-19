//
//   Copyright 2018-2020  SenX S.A.S.
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

import java.io.IOException;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.FetchRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

public interface StoreClient {
  public void store(GTSEncoder encoder) throws IOException;
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException;
  /**
   * @param req FetchRequest instance containing the following elements:
   *   token Read token to use for reading data
   *   metadatas List of Metadata for the GTS to fetch
   *   now End timestamp (included)
   *   thents Start timestamp (included)
   *   count Number of datapoints to fetch. 0 is a valid value if you want to fetch only boundaries. Use -1 to specify you are not fetching by count.
   *   skip Number of datapoints to skip before returning values
   *   step Index offset between two datapoints, defaults to 1, i.e. return every data point
   *   timestep Minimum time offset between datapoints, defaults to 1 time unit
   *   sample Double value representing the sampling rate. Use 1.0D for returning all values. Valid values are ] 0.0D, 1.0D ]
   *   writeTimestamp Flag indicating we are interested in the HBase cell timestamp
   *   preBoundary Size of the pre boundary in number of values
   *   postBoundary Size of the post boundary in number of values
   */
  public GTSDecoderIterator fetch(FetchRequest req) throws IOException;
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler);
}
