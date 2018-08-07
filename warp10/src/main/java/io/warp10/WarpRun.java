package io.warp10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Properties;

import com.google.common.base.Charsets;

import io.warp10.continuum.Configuration;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.functions.SNAPSHOT;

public class WarpRun {
  
  private static final String WARPRUN_FORMAT = "warprun.format";
  
  public static void main(String[] args) throws Exception {
    try {
      System.setProperty(Configuration.WARP10_QUIET, "true");
      System.setProperty(Configuration.WARPSCRIPT_REXEC_ENABLE, "true");
      
      if (null == System.getProperty(Configuration.WARP_TIME_UNITS)) {
        System.setProperty(Configuration.WARP_TIME_UNITS, "us");
      }
      
      WarpConfig.setProperties((String) null);
      
      Properties properties = WarpConfig.getProperties();
      
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, properties);
      stack.maxLimits();
      
      StringBuilder sb = new StringBuilder();
      
      BufferedReader br;
      
      if ("-".equals(args[0])) {
        br = new BufferedReader(new InputStreamReader(System.in, Charsets.UTF_8));
      } else {
        br = new BufferedReader(new FileReader(args[0]));
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
      
      stack.execMulti(sb.toString());      
      
      //
      // Output the stack in either JSON or SNAPSHOT format
      //
      
      boolean json = "json".equals(properties.getProperty(WARPRUN_FORMAT));
      
      if (!json) {
        SNAPSHOT snap = new SNAPSHOT("SNAPSHOT", false, false, false, false);
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
      } else {
        StackUtils.toJSON(new PrintWriter(System.out), stack);
        System.out.flush();
      }
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      System.exit(-1);
    }
  }
}
