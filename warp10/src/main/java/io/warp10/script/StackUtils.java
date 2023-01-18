//
//   Copyright 2018-2022  SenX S.A.S.
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

package io.warp10.script;

//import io.warp10.json.JsonUtils;
//import io.warp10.WarpURLEncoder;
//import io.warp10.continuum.gts.GTSHelper;
//import io.warp10.continuum.gts.GeoTimeSerie;
//import io.warp10.continuum.gts.UnsafeString;
//import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
//import io.warp10.continuum.store.thrift.data.Metadata;
//import io.warp10.script.WarpScriptStack.Macro;
//import io.warp10.script.functions.SNAPSHOT.Snapshotable;
//import io.warp10.warp.sdk.WarpScriptJavaFunctionGTS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.json.JsonUtils;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;

public class StackUtils {

  public static void toJSON(PrintWriter out, WarpScriptStack stack, int maxdepth, long maxJsonSize) throws WarpScriptException, IOException {

    boolean strictJSON = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_JSON_STRICT));
    boolean prettyJSON = Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_JSON_PRETTY));

    int depth = Math.min(stack.depth(), maxdepth);

    out.print("[");

    boolean first = true;

    for (int i = 0; i < depth; i++) {

      if (!first) {
        out.print(",");
      }
      first = false;

      Object o = stack.get(i);

      JsonUtils.objectToJson(out, o, strictJSON, prettyJSON, maxJsonSize);
    }

    out.print("]");
  }

  public static void toJSON(PrintWriter out, WarpScriptStack stack) throws WarpScriptException, IOException {
    toJSON(out, stack, Integer.MAX_VALUE, Long.MAX_VALUE);
  }

  /**
   * Sanitize a script instance, removing comments etc.
   * Inspired by MemoryWarpScriptStack#exec
   *
   * @param script The script to sanitize.
   * @return A StringBuilder containing the sanitized version of the script.
   */
  public static StringBuilder sanitize(String script) throws WarpScriptException {
    StringBuilder sb = new StringBuilder();

    if (null == script) {
      return sb;
    }

    //
    // Build a BufferedReader to access each line of the script
    //

    BufferedReader br = new BufferedReader(new StringReader(script));

    boolean inComment = false;

    try {
      while(true) {
        String line = br.readLine();

        if (null == line) {
          break;
        }

        line = line.trim();

        String[] statements;

        //
        // Replace whistespaces in Strings with '%20'
        //

        line = UnsafeString.sanitizeStrings(line);
        
        if (-1 != line.indexOf(' ')) {
          //statements = line.split(" +");
          statements = UnsafeString.split(line, ' ');
        } else {
          statements = new String[1];
          statements[0] = line;
        }

        //
        // Loop over the statements
        //

        for (String stmt: statements) {

          //
          // Skip empty statements
          //

          if (0 == stmt.length()) {
            continue;
          }

          //
          // Trim statement
          //

          stmt = stmt.trim();

          //
          // End execution on encountering a comment
          //

          if (stmt.charAt(0) == '#' || (stmt.charAt(0) == '/' && stmt.length() >= 2 && stmt.charAt(1) == '/')) {
            // Skip comments and blank lines
            break;
          }

          if (WarpScriptStack.COMMENT_END.equals(stmt)) {
            if (inComment) {
              throw new WarpScriptException("Not inside a comment.");
            }
            inComment = false;
            continue;
          } else if (inComment) {
            continue;
          } else if (WarpScriptStack.COMMENT_START.equals(stmt)) {
            inComment = true;
            continue;
          }

          if (sb.length() > 0) {
            sb.append(" ");
          }
          sb.append(stmt);
        }
      }

      br.close();

    } catch(IOException ioe) {
      throw new WarpScriptException(ioe);
    }

    return sb;
  }

  public static String toString(Object o) {
    StringBuilder sb = new StringBuilder();

    if (null == o) {
      sb.append("NULL");
    } else if (o instanceof Number) {
      sb.append(o);
    } else if (o instanceof String) {
      sb.append("'");
      try {
        sb.append(WarpURLEncoder.encode(o.toString(), StandardCharsets.UTF_8));
      } catch (UnsupportedEncodingException uee) {
      }
      sb.append("'");
    } else if (o instanceof Boolean) {
      sb.append(Boolean.toString((boolean) o));
    } else if (o instanceof WarpScriptStackFunction) {
      sb.append(o.toString());
    } else if (o instanceof Snapshotable) { // Also includes Macro
      ((Snapshotable) o).snapshot();
    } else if (o instanceof NamedWarpScriptFunction){
      sb.append(o.toString());
    }
    return sb.toString();
  }

}
