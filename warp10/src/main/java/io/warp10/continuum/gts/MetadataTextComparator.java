//
//   Copyright 2018-2021  SenX S.A.S.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.store.thrift.data.Metadata;

/**
 * Sort Metadata according to field values (labels, attributes or classname if null provided)
 */
public class MetadataTextComparator implements Comparator<Metadata> {

  private final List<String> fields;

  // This is taken into account when the fields list provided is non-null and non-empty
  // Then, if set to true, attributes are considered as labels
  private final boolean fieldsConsiderAttributes;

  public MetadataTextComparator(List<String> fields) {
    this.fields = fields;
    this.fieldsConsiderAttributes = false;
  }

  public MetadataTextComparator(List<String> fields, boolean fieldsConsiderAttributes) {
    this.fields = fields;
    this.fieldsConsiderAttributes = fieldsConsiderAttributes;
  }

  @Override
  public int compare(Metadata o1, Metadata o2) {

    if (null == o1 && null == o2) {
      return 0;
    }

    if (null == o1) {
      return -1;
    }

    if (null == o2) {
      return 1;
    }

    if (null != this.fields && !this.fields.isEmpty()) {
      return compareWithFields(o1, o2);
    }

    //
    // Compare name
    //
    String name1 = o1.getName();
    String name2 = o2.getName();

    if (null == name1 && null != name2) {
      return -1;
    }

    if (null == name2 && null != name1) {
      return 1;
    }

    int comp;
    if (null != name1) {
      // Both name are valid, compare them
      comp = name1.compareTo(name2);

      if (0 != comp) {
        return comp;
      }
    }

    //
    // Names are identical, compare labels
    //
    int size1 = o1.getLabelsSize();
    int size2 = o2.getLabelsSize();

    if (0 == size1 && 0 != size2) {
      return -1;
    }

    if (0 == size2 && 0 != size1) {
      return 1;
    }

    List<String> labels1 = new ArrayList<String>(size1);
    labels1.addAll(o1.getLabels().keySet());

    List<String> labels2 = new ArrayList<String>(size2);
    labels2.addAll(o2.getLabels().keySet());

    Collections.sort(labels1);
    Collections.sort(labels2);

    int idx = 0;

    while (idx < size1 && idx < size2) {

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

    // No differences found in label name and value, check the label list size
    // Check if the number of labels differ, min number is first
    if (size1 < size2) {
      return -1;
    }
    if (size1 > size2) {
      return 1;
    }

    //
    // Names are identical, labels are identical, compare attributes
    //

    size1 = o1.getAttributesSize();
    size2 = o2.getAttributesSize();

    if (0 == size1 && 0 == size2) {
      return 0;
    }

    if (0 == size1) {
      return -1;
    }

    if (0 == size2) {
      return 1;
    }


    List<String> attr1 = new ArrayList<String>(size1);
    attr1.addAll(o1.getAttributes().keySet());

    List<String> attr2 = new ArrayList<String>(size2);
    attr2.addAll(o2.getAttributes().keySet());

    Collections.sort(attr1);
    Collections.sort(attr2);

    idx = 0;

    while (idx < size1 && idx < size2) {

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

    // Check if the number of attributes differ, min number is first
    return Integer.compare(size1, size2);
  }


  private int compareWithFields(Metadata o1, Metadata o2) {

    Map<String, String> m1 = new HashMap<String, String>();
    Map<String, String> m2 = new HashMap<String, String>();

    if (this.fieldsConsiderAttributes) {
      m1.putAll(o1.getAttributes());
      m2.putAll(o2.getAttributes());
    }

    // Labels will overwrite attributes with the same name
    m1.putAll(o1.getLabels());
    m2.putAll(o2.getLabels());

    //
    // Loop over the fields
    //

    for (String field: this.fields) {
      //
      // Extract field from both o1 and o2
      // Field 'null' is the GTS name
      //

      String s1;
      String s2;

      if (null == field) {
        s1 = o1.getName();
        s2 = o2.getName();
      } else {
        s1 = m1.get(field);
        s2 = m2.get(field);
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

      int comp = s1.compareTo(s2);

      if (0 != comp) {
        return comp;
      }
    }
    return 0;
  }
}
