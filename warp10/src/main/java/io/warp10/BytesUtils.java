//
//   Copyright 2022-2023  SenX S.A.S.
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

package io.warp10;

import com.google.common.primitives.UnsignedBytes;

public class BytesUtils {
  public static int compareTo(final byte[] left, final byte[] right) {
    return UnsignedBytes.lexicographicalComparator().compare(left, right);
  }

  public static int compareTo(final byte[] left, int leftoffset, int leftlen, final byte[] right, int rightoffset, int rightlen) {
    // When comparing the entire arrays, fallback to compareTo(left,right) since it is significantly faster
    if (left.length == leftlen && right.length == rightlen && 0 == leftoffset && 0 == rightoffset) {
      return compareTo(left, right);
    }

    int minlen = leftlen;
    if (minlen > rightlen) {
      minlen = rightlen;
    }

    for (int i = 0; i < minlen; i++) {
      int lefti = ((int) left[i + leftoffset]) & 0xFF;
      int righti = ((int) right[i + rightoffset]) & 0xFF;

      if (lefti < righti) {
        return -1;
      } else if (lefti > righti) {
        return 1;
      }
    }

    // The two byte arrays are identical on the first minlen bytes
    // the shortest one will now win!

    return Integer.compare(left.length, right.length);
  }
}
