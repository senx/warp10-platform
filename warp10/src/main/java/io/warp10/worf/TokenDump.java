package io.warp10.worf;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;

public class TokenDump extends TokenGen {
  
  public static void main(String[] args) throws Exception {
    
    TokenDump instance = new TokenDump();
    
    instance.usage(args);

    instance.parse(args);
    
    instance.process(args);
  }

  @Override
  public void usage(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: TokenDump config in out");
      System.exit(-1);
    }
  }
  
  @Override
  public void process(String[] args) throws Exception {
    PrintWriter pw = new PrintWriter(System.out);
    
    if (args.length > 2) {
      if (!"-".equals(args[2])) {
        pw = new PrintWriter(new FileWriter(args[2]));
      }
    }
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
      
    byte[] buf = new byte[8192];
      
    InputStream in = null;
      
    if ("-".equals(args[1])) {
      in = System.in;
    } else {
      in = new FileInputStream(args[1]);
    }

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, WarpConfig.getProperties());
    stack.maxLimits();
    
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    
    boolean json = null != System.getProperty("json");
    
    while(true) {
      String line = br.readLine();
        
      if (null == line) {
        break;
      }
      
      stack.clear();
      stack.push(line);
      stack.exec("TOKENDUMP");
      
      if (json) {
        stack.exec("->JSON");
      } else {
        stack.exec("SNAPSHOT");
      }
      
      pw.println(stack.pop().toString());
    }
      
    br.close();
    
    pw.flush();
    pw.close();    
  }
}
