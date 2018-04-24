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

public class MAN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MAN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String functionname=null;

    //
    // if there is a string on the top of the stack, take is as a function name
    // if the function name is valid, returns the help URL.
    // if there is no stack is empty or top is not a string, returns WARP10_DOC_URL
    //

    if (0 < stack.depth()) {
      Object o = stack.pop();
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
