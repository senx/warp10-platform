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

import io.warp10.script.WarpScriptLib;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Generate .mc2 documentation and run warpscript unit tests
 */
public class UnitTestAndDocGenerationExample extends RunAndGenerateDocumentationWithUnitTests {

  //
  // Overridden test run parameters
  //

  protected boolean WRITE() {
    return false;
  }

  protected String OUTPUT_FOLDER() {
    return "/my/output/folder/";
  }

  protected boolean OVERWRITE() {
    return false;
  }

  protected List<String> TAGS() {
    List<String> tags = new ArrayList<>();
    //tags.add("some tag");
    return tags;
  }

  //
  // Register extension
  //

  static {
    // for example:
    // WarpScriptLib.register(new ArrowExtension());
  }

  //
  // Run test
  //

  @Test
  public void generate() throws Exception {
    generate(WarpScriptLib.getFunctionNames());
  }

}