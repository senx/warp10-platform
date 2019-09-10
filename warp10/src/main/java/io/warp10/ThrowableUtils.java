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
    Throwable trueRoot = t;

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
          message += " (" + causeMessage + ")";
        }
        // The root and the direct cause messages are enough, stop here.
        break;
      }
    }

    if (null == message) {
      // In case no message has been found, display the Throwable "user-friendly" classname.
      return trueRoot.getClass().getSimpleName();
    } else {
      return message;
    }
  }

}
