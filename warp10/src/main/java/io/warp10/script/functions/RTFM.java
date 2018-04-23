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

package io.warp10.script.functions;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptLib;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import io.warp10.crypto.OrderPreservingBase64;
import com.google.common.base.Charsets;

public class RTFM extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RTFM(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String functionname=null;

    //
    // if there is a string on the top of the stack, take is as a function name
    // if the function name is valid, returns the help URL.
    //
    // RTFM do not pop the string anyway.
    //

    if (0 < stack.depth()) {
      Object o = stack.peek();
      if (o instanceof String) {
        functionname = o.toString();
        if (!WarpScriptLib.getFunctionNames().contains(functionname)) {
          functionname="";
        }
      }
    }

    if (null == functionname) {
      stack.push(Constants.WARP10_DOC_URL);
    }
    else if ( "" == functionname) {
      stack.push("Unknown function name, please check " + Constants.WARP10_DOC_URL);
    }
    else {
      String forbiddenindoc = ".*[!%&\\(\\)\\*+/<=>\\[\\]^\\{\\|\\}~].*";
      Pattern pattern = Pattern.compile(forbiddenindoc);
      String docname = functionname;
      Matcher m = pattern.matcher(functionname);
      if (m.matches()
          || "-".equals(functionname)
          || "pi".equals(functionname)
          || "PI".equals(functionname)
          || "e".equals(functionname)
          || "E".equals(functionname)
          || "Pfilter".equals(functionname)
          ) {
        //contains forbidden characters, or is a name exception.
        docname = new String(OrderPreservingBase64.encode(functionname.getBytes(Charsets.UTF_8)), Charsets.UTF_8);
      }
      stack.push(Constants.WARP10_FUNCTION_DOC_URL + docname);
    }

    return stack;
  }
}
