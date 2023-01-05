//
//   Copyright 2018-2023  SenX S.A.S.
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
package io.warp10;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import io.warp10.continuum.Configuration;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.ext.warprun.WarpRunWarpScriptExtension;
import io.warp10.script.functions.SNAPSHOT;

public class WarpRun {

  private static final String WARPRUN_FORMAT = "warprun.format";

  public static void main(String[] args) throws Exception {
    try {
      System.setProperty(Configuration.WARP10_QUIET, "true");
      System.setProperty(Configuration.WARPSCRIPT_REXEC_ENABLE, "true");
      System.setProperty(Configuration.CONFIG_DEBUG_CAPABILITY, "false");

      if (null == System.getProperty(Configuration.WARP_TIME_UNITS)) {
        System.setProperty(Configuration.WARP_TIME_UNITS, "us");
      }

      //
      // Initialize WarpConfig
      //

      try {
        String config = System.getProperty(WarpConfig.WARP10_CONFIG);

        if (null == config) {
          config = System.getenv(WarpConfig.WARP10_CONFIG_ENV);
        }

        String[] files = null;

        if (null != config) {
          files = config.split("[, ]+");
        }

        WarpConfig.safeSetProperties(files);
        WarpScriptLib.registerExtensions();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      Properties properties = WarpConfig.getProperties();

      //
      // Register the WarpRun and Debug extensions
      //

      WarpScriptLib.register(new WarpRunWarpScriptExtension());

      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, properties);
      stack.maxLimits();

      StringBuilder sb = new StringBuilder();

      BufferedReader br;

      if ("-".equals(args[0])) {
        br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      } else {
        br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), StandardCharsets.UTF_8));
      }

      while(true) {
        String line = br.readLine();

        if (null == line) {
          break;
        }

        sb.append(line);
        sb.append("\n");
      }

      br.close();

      //
      // Push parameters
      //

      if (args.length > 1) {
        for (int i = 1; i < args.length; i++) {
          stack.push(args[i]);
        }
      }

      stack.execMulti(sb.toString());

      //
      // Output the stack in either JSON or SNAPSHOT format
      //

      boolean json = "json".equals(WarpConfig.getProperty(WARPRUN_FORMAT));
      boolean stdout = "stdout".equals(WarpConfig.getProperty(WARPRUN_FORMAT));

      if (stdout) {
        // Do nothing, STDOUT is handled by the script
      } else if (json) {
        PrintWriter p = new PrintWriter(System.out);
        StackUtils.toJSON(p, stack);
        p.flush();
      } else {
        SNAPSHOT snap = new SNAPSHOT(WarpScriptLib.SNAPSHOT, false, false, false, false);
        for (int i = stack.depth() - 1; i >=0; i--) {
          System.out.print("/* ");
          if (0 != i) {
            System.out.print(i + 1);
          } else {
            System.out.print(" TOP ");
          }
          System.out.print(" */  ");
          sb.setLength(0);
          SNAPSHOT.addElement(snap, sb, stack.get(i));
          System.out.println(sb.toString());
        }
      }
      System.out.flush();
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      System.exit(-1);
    }
  }
}
