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

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class FormattedWarpScriptFunctionTest extends FormattedWarpScriptFunction {

  public FormattedWarpScriptFunctionTest() {
    super("EXAMPLE");

    //
    // Positional arguments
    //

    List<ArgumentSpecification> args = getArguments();
    args.add(new ArgumentSpecification(GeoTimeSerie.class, "1st arg","A GTS."));
    args.add(new ArgumentSpecification(Long.class, "2nd arg","A LONG."));
    args.add(new ArgumentSpecification(Double.class, "3rd arg","A DOUBLE."));

    //
    // Optional arguments
    //

    List<ArgumentSpecification> optArgs = getOptionalArguments();
    optArgs.add(new ArgumentSpecification(String.class, "1st opt arg", "The default value.", "A STRING."));

    //
    // Optional doc
    //

    StringBuilder docstring = getDocstring();
    docstring.append("This is an example implementation of FormattedWarpScriptFunction. " + getName() + " returns a map of its arguments.");

    //
    // Optional unit tests
    //

    List<String> unitTests = getUnitTests();

    StringBuilder test1 = new StringBuilder();
    test1.append("NEWGTS 3 0.5 EXAMPLE 'res' STORE" + System.lineSeparator());
    test1.append("$res '1st arg' GET TYPEOF 'GTS' == ASSERT" + System.lineSeparator());
    test1.append("$res '2nd arg' GET 3 == ASSERT" + System.lineSeparator());
    test1.append("$res '3rd arg' GET 0.5 == ASSERT" + System.lineSeparator());
    test1.append("$res SIZE 4 == ASSERT" + System.lineSeparator());
    test1.append("$res '1st opt arg' GET 'The default value.' == ASSERT");
    unitTests.add(test1.toString());

    StringBuilder test2 = new StringBuilder();
    test2.append("{ '1st arg' NEWGTS '2nd arg' 3 '3rd arg' 0.5 } EXAMPLE 'res' STORE" + System.lineSeparator());
    test2.append("$res '1st arg' GET TYPEOF 'GTS' == ASSERT" + System.lineSeparator());
    test2.append("$res '2nd arg' GET 3 == ASSERT" + System.lineSeparator());
    test2.append("$res '3rd arg' GET 0.5 == ASSERT" + System.lineSeparator());
    test2.append("$res SIZE 4 == ASSERT" + System.lineSeparator());
    test2.append("$res '1st opt arg' GET 'The default value.' == ASSERT");
    unitTests.add(test2.toString());

    StringBuilder test3 = new StringBuilder();
    test3.append("{ '1st arg' NEWGTS '2nd arg' 3 '3rd arg' 0.5 '1st opt arg' 'hi' } EXAMPLE 'res' STORE" + System.lineSeparator());
    test3.append("$res '1st arg' GET TYPEOF 'GTS' == ASSERT" + System.lineSeparator());
    test3.append("$res '2nd arg' GET 3 == ASSERT" + System.lineSeparator());
    test3.append("$res '3rd arg' GET 0.5 == ASSERT" + System.lineSeparator());
    test3.append("$res '1st opt arg' GET 'hi' == ASSERT" + System.lineSeparator());
    test3.append("$res SIZE 4 == ASSERT");
    unitTests.add(test3.toString());

  }

  //
  // The application of this function on the stack
  // formattedArgs contains positional and optional arguments
  //

  protected WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException {

    stack.push(formattedArgs);
    return stack;
  }

  //
  // Running unit tests
  //

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.setProperties(new StringReader(props.toString()));

    WarpScriptLib.addNamedWarpScriptFunction(new FormattedWarpScriptFunctionTest());
  }

  @Test
  public void test1() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);

    stack.execMulti(getUnitTests().get(0));
  }

  @Test
  public void test2() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);

    stack.execMulti(getUnitTests().get(1));
  }

  @Test
  public void test3() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);

    stack.execMulti(getUnitTests().get(2));
  }

}