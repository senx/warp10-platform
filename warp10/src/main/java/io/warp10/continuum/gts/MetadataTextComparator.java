//
//   Copyright 2018 - 2021  SenX S.A.S.
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

package io.warp10.continuum.gts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import io.warp10.continuum.store.thrift.data.Metadata;

/**
 * Sort Metadata according to field values (labels, attributes or classname if null provided)
 */
public class MetadataTextComparator implements Comparator<Metadata> {

  private final List<String> fields;

  public MetadataTextComparator(List<String> fields) {
    this.fields = fields;
  }

  @Override
  public int compare(Metadata o1, Metadata o2) {

    if (null != this.fields && !this.fields.isEmpty()) {
      return compareWithFields(o1, o2);
    }

    if (null == o1) {
      return -1;
    }

    if (null == o2) {
      return 1;
    }

    String name1 = o1.getName();
    String name2 = o2.getName();

    if (null == name1) {
      return -1;
    }

    if (null == name2) {
      return 1;
    }

    int comp = name1.compareTo(name2);

    if (0 != comp) {
      return comp;
    }

    //
    // Names are identical, compare labels
    //

    if (0 == o1.getLabelsSize() && 0 == o2.getLabelsSize()) {
      return 0;
    }

    if (0 == o1.getLabelsSize()) {
      return -1;
    }

    if (0 == o2.getLabelsSize()) {
      return 1;
    }

    List<String> labels1 = new ArrayList<String>(o1.getLabelsSize());
    labels1.addAll(o1.getLabels().keySet());

    List<String> labels2 = new ArrayList<String>(o2.getLabelsSize());
    labels2.addAll(o2.getLabels().keySet());

    Collections.sort(labels1);
    Collections.sort(labels2);

    int idx = 0;

    while (idx < labels1.size() && idx < labels2.size()) {

      // Compare label names
      comp = labels1.get(idx).compareTo(labels2.get(idx));

      if (0 != comp) {
        return comp;
      }

      // Compare label values
      comp = o1.getLabels().get(labels1.get(idx)).compareTo(o2.getLabels().get(labels2.get(idx)));

      if (0 != comp) {
        return comp;
      }

      // Advance labels
      idx++;
    }

    //
    // Names are identical, labels are identical, compare attributes
    //
    
    if (0 == o1.getAttributesSize() && 0 == o2.getAttributesSize()) {
      return 0;
    }

    if (0 == o1.getAttributesSize()) {
      return -1;
    }

    if (0 == o2.getAttributesSize()) {
      return 1;
    }
    

    List<String> attr1 = new ArrayList<String>(o1.getAttributesSize());
    attr1.addAll(o1.getAttributes().keySet());

    List<String> attr2 = new ArrayList<String>(o2.getAttributesSize());
    attr2.addAll(o2.getAttributes().keySet());

    Collections.sort(attr1);
    Collections.sort(attr2);

    idx = 0;

    while (idx < attr1.size() && idx < attr2.size()) {

      // Compare attribute names
      comp = attr1.get(idx).compareTo(attr2.get(idx));

      if (0 != comp) {
        return comp;
      }

      // Compare attribute values
      comp = o1.getAttributes().get(attr1.get(idx)).compareTo(o2.getAttributes().get(attr2.get(idx)));

      if (0 != comp) {
        return comp;
      }

      // Advance attributes
      idx++;
    }

    return 0;
  }


  private int compareWithFields(Metadata o1, Metadata o2) {

    //
    // Loop over the fields
    //

    for (String field: this.fields) {
      //
      // Extract field from both o1 and o2
      // Field 'null' is the GTS name
      //

      String s1 = null;
      String s2 = null;

      if (null == field) {
        s1 = o1.getName();
        s2 = o2.getName();
      } else {
        if (o1.getLabelsSize() > 0) {
          s1 = o1.getLabels().get(field);
        }
        if (o2.getLabelsSize() > 0) {
          s2 = o2.getLabels().get(field);
        }
      }

      if (null == s1 && null != s2) {
        return -1;
      }

      if (null == s2 && null != s1) {
        return 1;
      }

      // Here, if s1 is null, s2 is null too
      if (null == s1) {
        if (o1.getAttributesSize() > 0) {
          s1 = o1.getAttributes().get(field);

        }
        if (o2.getAttributesSize() > 0) {
          s2 = o2.getAttributes().get(field);
        }

        if (null == s1 && null != s2) {
          return -1;
        }

        if (null == s2 && null != s1) {
          return 1;
        }

        // Here, if s1 is null, s2 is null too
        if (null == s1) {
          continue;
        }
      }

      int comp = s1.compareTo(s2);

      if (0 != comp) {
        return comp;
      }
    }

    return 0;
  }
}
