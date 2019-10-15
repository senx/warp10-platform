//
//   Copyright 2019  SenX S.A.S.
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

import java.util.ArrayList;

public class ThrowableUtils {

  public static String getErrorMessage(Throwable t) {
    return getErrorMessage(t, Integer.MAX_VALUE);
  }

  public static String getErrorMessage(Throwable t, int maxSize) {
    String simpleClassName = t.getClass().getSimpleName();

    if (maxSize <= 9) { // Not even enough for "... (...)"
      return simpleClassName.substring(0, Math.min(maxSize, simpleClassName.length()));
    }

    // Maintain a list of Throwable causes to avoid the unlikely case of a cycle in causes.
    final ArrayList<Throwable> throwables = new ArrayList<>();
    throwables.add(t);

    // First, simply get the message of the root Throwable
    String message = t.getMessage();

    // If there is a cause to this Throwable, find if adding it is a good option.
    while (null != t.getCause() && !throwables.contains(t.getCause())) {
      // Test if the message of the root Throwable is simply the toString of the cause or its message.
      // The former happens when the cause Throwable has been passed as a parameter to the root Throwable.
      if (null == message || message.equals(t.getCause().toString()) || message.equals(t.getCause().getMessage())) {
        // In that case, consider the cause as the root and loop.
        t = t.getCause();
        throwables.add(t);
        message = t.getMessage();
      } else {
        // If not, add the cause between braces to the root Throwable message.
        String causeMessage = t.getCause().getMessage();
        if (null != causeMessage && !"".equals(causeMessage)) {
          String toAppend = " (" + causeMessage + ")";

          // If the concatenation of message and addition is more than maxSize, truncate one of them or both.
          if (message.length() + toAppend.length() > maxSize) {
            if (message.length() <= maxSize / 2) {
              // Only toAppend is too long, truncate it.
              toAppend = toAppend.substring(0, maxSize - message.length() - 4) + "...)";
            } else if (toAppend.length() <= maxSize / 2) {
              // Only message is too long, truncate it.
              message = message.substring(0, maxSize - toAppend.length() - 3) + "...";
            } else {
              // Both message and toAppend too long, truncate both.
              toAppend = toAppend.substring(0, maxSize / 2 - 4) + "...)";
              message = message.substring(0, maxSize - toAppend.length() - 3) + "...";
            }
          }

          message += toAppend;
        }
        // The root and the direct cause messages are enough, stop here.
        break;
      }
    }

    if (null == message) {
      // In case no message has been found, display the Throwable "user-friendly" classname.
      message = simpleClassName;
    }

    if (message.length() > maxSize) {
      // Too long message, truncate. This case can only happen if the Throwable has no cause.
      message = message.substring(0, maxSize - 3) + "...";
    }

    return message;
  }

}
