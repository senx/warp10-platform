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

package io.warp10.script.functions;

import com.vividsolutions.jts.util.Assert;
import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SRANDPDFtest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }

  @Test
  public void test_SRANDPDF() throws Exception {

    //
    // Test seed rng from histogram
    //

    Random prng = new Random();
    int size = 100;
    Map<Long, Double> histogram = new HashMap<Long, Double>(size);
    for (int i = 0; i < size; i++) {
      histogram.put(new Long(i), prng.nextDouble());
    }

    //
    // RNG 1
    //

    MemoryWarpScriptStack stack_1 = new MemoryWarpScriptStack(null, null);
    stack_1.maxLimits();

    stack_1.push(123456789L);
    stack_1.exec(WarpScriptLib.PRNG);
    stack_1.push(histogram);
    stack_1.exec(WarpScriptLib.SRANDPDF);
    WarpScriptStackFunction rng_1 = (WarpScriptStackFunction) stack_1.pop();

    //
    // RNG 2
    //

    MemoryWarpScriptStack stack_2 = new MemoryWarpScriptStack(null, null);
    stack_2.maxLimits();

    stack_2.push(123456789L);
    stack_2.exec(WarpScriptLib.PRNG);
    stack_2.push(histogram);
    stack_2.exec(WarpScriptLib.SRANDPDF);
    WarpScriptStackFunction rng_2 = (WarpScriptStackFunction) stack_2.pop();

    //
    // Compare them
    //

    int N = 100;
    for (int i = 0; i < N; i++) {
      rng_1.apply(stack_1);
      rng_2.apply(stack_2);

      Assert.equals(stack_1.pop(), stack_2.pop());
    }

  }

}
