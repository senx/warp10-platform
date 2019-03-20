package io.warp10.script.formatted;

import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.SNAPSHOT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.warp10.script.WarpScriptStack.MACRO_END;
import static io.warp10.script.WarpScriptStack.MACRO_START;

public class DocumentationGenerator {

  //
  // Automatically generate doc of a formatted WarpScript function.
  //

  public static Map<String, Object> generateInfo(FormattedWarpScriptFunction function, List<ArgumentSpecification> outputs) {
    return generateInfo(function,"","","","", new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), outputs);
  }

  public static Map<String, Object> generateInfo(FormattedWarpScriptFunction function,
                                                 String since, String deprecated, String deleted, String version,
                                                 List<String> tags, List<String> related, List<String> examples, List<String> conf,
                                                 List<ArgumentSpecification> outputs) {

    HashMap<String, Object> info = new HashMap<>();

    info.put("name", function.getName());
    info.put("since", since);
    info.put("deprecated", deprecated);
    info.put("deleted", deleted);
    info.put("version", version);
    info.put("tags", tags);
    info.put("desc", function.getDocstring().toString());
    info.put("related", related);
    info.put("examples", examples);
    info.put("conf", conf);

    //
    // Params generation
    //

    HashMap<String, String> params = new HashMap<>();
    for (ArgumentSpecification arg: function.getArguments()) {
      params.put(arg.getName(), arg.getDoc());
    }
    for (ArgumentSpecification arg: function.getOptionalArguments()) {
      params.put(arg.getName(), arg.getDoc());
    }
    for (ArgumentSpecification arg: outputs) {
      params.put(arg.getName(), arg.getDoc());
    }

    info.put("params", params);

    //
    // Signature generation
    //

    List<List<List<Object>>> sig = new ArrayList<>();
    List<Object> output = new ArrayList<>();
    for (ArgumentSpecification arg: outputs) {
      output.add(arg.getName() + ":" + arg.WarpScriptType());
    }

    //
    // Sig without opt args on the stack
    //

    List<List<Object>> sig1 = new ArrayList<>();
    List<Object> input1 = new ArrayList<>();

    if (0 == function.getArguments().size() && 0 != function.getOptionalArguments().size()) {
      input1.add(new HashMap<>());
    }

    for (ArgumentSpecification arg: function.getArguments()) {
      input1.add(arg.getName() + ":" + arg.WarpScriptType());
    }

    sig1.add(input1);
    sig1.add(output);
    sig.add(sig1);

    //
    // Sig with opt args on the stack (in a map)
    //

    List<List<Object>> sig2 = new ArrayList<>();
    List<Object> input2 = new ArrayList<>();
    HashMap<String, String> optMap = new HashMap<>();

    for (ArgumentSpecification arg: function.getArguments()) {
      optMap.put(arg.getName(), arg.getName() + ":" + arg.WarpScriptType());
    }

    for (ArgumentSpecification arg: function.getOptionalArguments()) {
      optMap.put(arg.getName(), arg.getName() + ":" + arg.WarpScriptType());
    }

    input2.add(optMap);
    sig2.add(input2);
    sig2.add(output);
    sig.add(sig2);

    info.put("sig", sig);

    return info;
  }

  public static String generateWarpScriptDoc(FormattedWarpScriptFunction function, List<ArgumentSpecification> outputs) throws WarpScriptException {
    return generateWarpScriptDoc(function,"","","","", new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), outputs);
  }

  public static String generateWarpScriptDoc(FormattedWarpScriptFunction function,
                                             String since, String deprecated, String deleted, String version,
                                             List<String> tags, List<String> related, List<String> examples, List<String> conf,
                                             List<ArgumentSpecification> outputs) throws WarpScriptException {

    StringBuilder mc2 = new StringBuilder();

    mc2.append(MACRO_START + System.lineSeparator());
    SNAPSHOT.addElement(mc2, generateInfo(function, since, deprecated, deleted, version, tags, related, examples, conf, outputs));
    mc2.append(System.lineSeparator());
    mc2.append("INFO" + System.lineSeparator());
    mc2.append(function.getName() + System.lineSeparator());
    mc2.append(MACRO_END + System.lineSeparator());
    mc2.append("'macro' STORE" + System.lineSeparator());
    mc2.append("// Unit tests" + System.lineSeparator());

    for (String unitTest: function.getUnitTests()) {
      mc2.append(unitTest + System.lineSeparator());
    }

    mc2.append("$macro" + System.lineSeparator());

    return mc2.toString();
  }
}
