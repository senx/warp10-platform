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
import io.warp10.script.WarpScriptLib;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;

public class UnitTestAndDocGenerationTest extends RunAndGenerateDocumentationWithUnitTests {

  @BeforeClass
  final public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }

  @Test
  public void generate() throws Exception {
    generate(WarpScriptLib.getFunctionNames());
  }
}
