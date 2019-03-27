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

package io.warp10.script.formatted;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Do the extraction of arguments from the stack in a formatted manner.
 * Arguments inside collections still have to be handled manually.
 *
 * Alternatively, a Map that contains all arguments and optional arguments can be provided on top of the stack.
 *
 * See FormattedWarpScriptFunctionTest for an example of child class.
 */
public abstract class FormattedWarpScriptFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final StringBuilder docstring;
  private final List<String> unitTests;

  public static class Arguments {
    private final List<ArgumentSpecification> args;
    private final List<ArgumentSpecification> optArgs;

    private Arguments(List<ArgumentSpecification> args, List<ArgumentSpecification> optArgs) {
      this.args = args;
      this.optArgs = optArgs;
    }

    public List<ArgumentSpecification> getArgsCopy() {
      return new ArrayList<>(args);
    }

    public List<ArgumentSpecification> getOptArgsCopy() {
      return new ArrayList<>(optArgs);
    }
  }

  public static class ArgumentsBuilder {
    private final List<ArgumentSpecification> args;
    private final List<ArgumentSpecification> optArgs;

    public ArgumentsBuilder() {
      args = new ArrayList<>();
      optArgs = new ArrayList<>();
    }

    public ArgumentsBuilder addArgument(Class<?> clazz, String name, String doc) {
      args.add(new ArgumentSpecification(clazz, name, doc));
      return this;
    }

    public ArgumentsBuilder addOptionalArgument(Class<?> clazz, String name, Object defaultValue, String doc) {
      optArgs.add(new ArgumentSpecification(clazz, name, defaultValue, doc));
      return this;
    }

    public Arguments build() {
      return new Arguments(args, optArgs);
    }
  }

  public FormattedWarpScriptFunction(String name) {
    super(name);

    //
    // A default child class has 0 argument, 0 optional argument, an empty docstring, and 0 unit tests.
    //

    docstring = new StringBuilder();
    unitTests = new ArrayList<String>();
  }

  //
  // A child class provides the specs of its arguments by building an instance of Arguments in its constructor.
  //

  protected abstract Arguments getArguments();

  //
  // A child class uses formattedArgs to apply its function's logic, and pushes its outputs onto the stack.
  //

  protected abstract WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException;

  //
  // Optionally, the child class can fill the fields docstring and unitTests for doc generation purpose.
  //

  protected final StringBuilder getDocstring() {
    return docstring;
  }

  protected List<String> getUnitTests() {
    return unitTests;
  }

  //
  // Handle arguments parsing
  //

  /**
   * Create the formatted args from the signatures provided by the child class, and apply child's apply method.
   *
   * @param stack
   * @return stack
   * @throws WarpScriptException
   */
  @Override
  final public Object apply(WarpScriptStack stack) throws WarpScriptException {

    List<ArgumentSpecification> args = getArguments().args;
    List<ArgumentSpecification> optArgs = getArguments().optArgs;

    //
    // Sanity checks
    //

    if (null == args) {
      throw new WarpScriptException(getClass().getCanonicalName() + "'s method getArguments() returned null. If no " +
        "argument is expected, it should return an empty array instead.");
    }

    if (null == optArgs) {
      throw new WarpScriptException(getClass().getCanonicalName() + "'s method getOptionalArguments() returned null." +
        " If no argument is expected, it should return an empty array instead.");
    }

    for (ArgumentSpecification arg: args) {
      if (arg.isOptional()) {
        throw new IllegalStateException("Output of " + getClass().getCanonicalName() + "'s method getArguments() must" +
          " only contain arguments without a default value.");
      }
    }

    for (ArgumentSpecification arg: optArgs) {
      if (!arg.isOptional()) {
        throw new IllegalStateException("Output of " + getClass().getCanonicalName() + "'s method getArguments() must" +
          " only contain arguments with a default value.");
      }
    }

    //
    // If args and opt args are empty, should expect nothing on top
    // If args is empty but opt args is not, should expect a map (possibly empty) on top
    //

    if (0 == args.size() && 0 == optArgs.size()) {
      return apply(new HashMap<String, Object>(), stack);
    }

    if (0 == args.size()) {

      if (0 == stack.depth() || !(stack.peek() instanceof Map)) {
        throw new WarpScriptException(getClass().getCanonicalName() + " expects a MAP on top of the stack. To use default argument values, an empty MAP is expected.");
      }
    }

    //
    // If there are possible optional arguments, check that last mandatory argument is not a Map so that there is no
    // confusion
    //

    if (args.size() > 1 && args.get(args.size() - 1) instanceof Map && optArgs.size() > 0) {
      throw new IllegalStateException("In this case, the last non-optional argument can not be a Map so as to avoid any confusion when using optional arguments.");
    }

    //
    // Extract arguments off the top of the stack
    //

    Map<String, Object> formattedArgs;

    if (stack.peek() instanceof Map) {

      //
      // Case 1: A map is on top of the stack (some optional arguments may be given)
      //

      Map<String, Object> map = (Map) stack.peek();

      //
      // Check that non-optional args are contained in the map and that they have the correct type
      //

      for (ArgumentSpecification arg: args) {
        if (!map.containsKey(arg.getName())) {

          throw new WarpScriptException("The MAP that is on top of the stack does not have the argument '" + arg.getName() +
            "' (of type "  + arg.WarpScriptType() + ") that is required by " + getName());
        }

        if (!arg.getClazz().isInstance(map.get(arg.getName()))) {

          throw new WarpScriptException(getClass().getCanonicalName() + " expects the argument '" + arg.getName() + "' to" +
            " be a " + arg.WarpScriptType() + ".");
        }
      }

      //
      // Check that optional args that are in the map are of the correct type
      //

      for (ArgumentSpecification arg: optArgs) {
        if (map.containsKey(arg.getName()) && !arg.getClazz().isInstance(map.get(arg.getName()))) {

          throw new WarpScriptException(getClass().getCanonicalName() + " expects the argument '" + arg.getName() + "' to " +
            "be a " + arg.WarpScriptType() + ".");
        }
      }

      //
      // Consume the top of the stack
      //

      formattedArgs = new HashMap<String, Object> ((Map) stack.pop());

    } else {

      //
      // Case 2: No optional argument are given
      //

      if (stack.depth() < args.size()) {
        throw new WarpScriptException(getClass().getCanonicalName() + " expects to find " + args.size() + " arguments" +
          " off the top of the stack, but the stack contains only " + stack.depth() + " levels.");
      }

      //
      // Check argument types
      //

      for (int i = 0; i < args.size(); i++) {
        ArgumentSpecification arg = args.get(args.size() - 1 - i);
        Object candidate = stack.get(i);

        if (!arg.getClazz().isInstance(candidate)) {
          throw new WarpScriptException(getClass().getCanonicalName() + " expects to find a '" + arg.getName() + "' (a " +
            arg.WarpScriptType() + ") " + leveldenomination(i));
        }
      }

      //
      // Consume these arguments off the top of the stack
      //

      formattedArgs = new HashMap<String,Object>();

      for (int i = 0; i < args.size(); i++) {
        ArgumentSpecification arg = args.get(args.size() - 1 - i);
        formattedArgs.put(arg.getName(), stack.pop());
      }

    }

    //
    // Set absent optional arguments to their default values
    //

    for (ArgumentSpecification arg: optArgs) {
      if (!formattedArgs.containsKey(arg.getName())) {
        formattedArgs.put(arg.getName(), arg.getDefaultValue());
      }
    }

    return apply(formattedArgs, stack);
  }

  final private String leveldenomination(int i) {
    if (i < 0) {
      throw new IllegalStateException("Can not be negative");
    }

    if (0 == i) {
      return "on top of the stack.";
    } else if (1 == i) {
      return "below the top of the stack.";
    } else if (2 == i) {
      return "on 3rd position counting from the top of the stack.";
    } else {
      return "on " + (i + 1) + "th position counting from the top of the stack.";
    }
  }
}