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

namespace java io.warp10.continuum.index.thrift.data

enum IndexComponentType {
  CLASS,
  LABELS,
  TIME,
  GEO,
  SUBLABELS,
  SUBCLASS,
  BINARY, // Verbatim
}

struct IndexComponent {
  /**
   * Type of component
   */
  1: required IndexComponentType type,
  
  /**
   * Modulus to apply to the timestamp in the index key (type TIME)
   */
  2: optional i64 modulus,
  
  /**
   * Resolution to which to constrain GeoXP Points in the index key (type GEO)
   */
  3: optional i32 resolution,

  /**
   * Labels to consider when computing this component of the index key (type SUBLABELS)
   */
  4: optional set<string> labels,
  
  /**
   * Number of class name levels to consider in the index key (type SUBCLASS)
   */  
  5: optional i32 levels,  
  
  /**
   * Optional pre-computed key component associated with this index component.
   * This is for example useful for class/labels/sublabels/subclass components, the key element
   * can be computed once for a given classId/labelsId.
   */ 
  6: optional binary key,
}

struct IndexSpec {
  /**
   * Unique Id of the index.
   * This is computer as the SipHash of <owner><index spec>
   * where <index spec> is the canonical textual represenation of the index
   */
  1: i64 indexId,
  
  /**
   * Index owner, the uuid of the user who created it
   */
  2: string owner,
  
  /**
   * Components present in the index key
   */
  3: list<IndexComponent> components,
  
  /**
   * Selector for selecting GTS instances to which the index applies (syntax is CLASS_SELECTOR{LABELS_SELECTORS})
   */
  4: optional string selector,
  
  /**
   * Size of generated key in bytes. This is useful for fast allocation of buffers
   */
  5: optional i32 keyLength,
}
