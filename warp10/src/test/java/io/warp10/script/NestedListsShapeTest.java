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

package io.warp10.script;

import io.warp10.WarpConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class NestedListsShapeTest {

  private static final Path FILE_1 = Paths.get("src", "test", "warpscript", "nestedList_0.mc2");

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }

  @Test
  public void testGet() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("'a' STORE");
    stack.execMulti("0 4 <% 'i' STORE");
    stack.execMulti("0 3 <% 'j' STORE");
    stack.execMulti("0 3 <% 'k' STORE");
    stack.execMulti("0 2 <% 'l' STORE");
    stack.execMulti("$a [ $i $j $k $l ] GET");
    stack.execMulti("$l $k 3 * + $j 3 * 4 * + $i 3 * 4 * 4 * +");
    stack.execMulti("== ASSERT");
    stack.execMulti("%> FOR %> FOR %> FOR %> FOR");
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testPut() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("4.3 [ 4 3 2 1 ] PUT DUP [ 4 3 2 1 ] GET 4.3 == ASSERT ");
    stack.execMulti("7.5 [ 1 0 2 2 ] PUT [ 1 0 2 2 ] GET 7.5 == ASSERT ");
    stack.execMulti("DEPTH 0 == ASSERT");
  }


  @Test
  public void testSet() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("4.3 [ 4 3 2 1 ] SET DUP [ 4 3 2 1 ] GET 4.3 == ASSERT ");
    stack.execMulti("7.5 [ 1 0 2 2 ] SET [ 1 0 2 2 ] GET 7.5 == ASSERT ");
    stack.execMulti("DEPTH 0 == ASSERT");
  }
}
