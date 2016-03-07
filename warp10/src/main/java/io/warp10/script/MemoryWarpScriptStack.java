//
//   Copyright 2016  Cityzen Data
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
import io.warp10.continuum.Configuration;
import io.warp10.continuum.geo.GeoDirectoryClient;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.functions.SECURE;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.WarpScriptJavaFunction;
import io.warp10.warp.sdk.WarpScriptJavaFunctionException;
import io.warp10.warp.sdk.WarpScriptRawJavaFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.util.Progressable;

public class MemoryWarpScriptStack implements WarpScriptStack, Progressable {

  private final AtomicLong[] counters;
  
  /**
   * Default maximum depth of the stack
   */
  private long maxdepth = 0L;

  /**
   * Maximum number of operations for this stack
   */
  private long maxops = 0L;

  /**
   * Maximum number of entries in the symbol table
   */
  private long maxsymbols = 0L;
  
  /**
   * Maximum recursion level
   */
  private long maxrecurse = 0L;
  
  /**
   * Current number of operations on this stack.
   * FIXME(hbs): use an AtomicLong is exec is to be called in an MT environment
   */
  private long currentops = 0L;
  
  private final List<Object> list = new ArrayList<Object>(32);

  private final Map<String,Object> symbolTable = new HashMap<String,Object>();
  
  /**
   * Map of stack attributes. This is used to store various values such as
   * limits or formats.
   */
  private final Map<String,Object> attributes = new HashMap<String,Object>();
  
  private StoreClient storeClient;
  
  private DirectoryClient directoryClient;
  
  private GeoDirectoryClient geoDirectoryClient;
  
  private final AtomicInteger recursionLevel = new AtomicInteger(0);
  
  private final String uuid = UUID.randomUUID().toString();
  
  /**
   * Vector to hold macros
   */
  private final List<Macro> macros = new ArrayList<Macro>();

  /**
   * StringBuilder to construct secure script
   */
  private StringBuilder secureScript = null;
  
  private AtomicBoolean inComment = new AtomicBoolean(false);

  private AtomicBoolean inMultiline = new AtomicBoolean(false);
  
  private StringBuilder multiline;

  /**
   * (re)defined functions
   */
  private Map<String,WarpScriptStackFunction> defined = new HashMap<String, WarpScriptStackFunction>();
  
  /**
   * Optional Hadoop progressable
   */
  private Progressable progressable = null;
  
  public static class StackContext extends WarpScriptStack.StackContext {
    public Map<String, Object> symbolTable;
    public Map<String, WarpScriptStackFunction> defined;
  }
  
  public StoreClient getStoreClient() {
    return this.storeClient;
  }
  
  public DirectoryClient getDirectoryClient() {
    return this.directoryClient;
  }
  
  public GeoDirectoryClient getGeoDirectoryClient() {
    return this.geoDirectoryClient;
  }
  
  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient) {
    this(storeClient, directoryClient, WarpConfig.getProperties());
  }

  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient, GeoDirectoryClient geoDirectoryClient) {
    this(storeClient, directoryClient, geoDirectoryClient, WarpConfig.getProperties());
  }

  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient, Properties properties) {
    this(storeClient, directoryClient, null, properties);
  }
  
  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient, GeoDirectoryClient geoDirectoryClient, Properties properties) {
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    this.geoDirectoryClient = geoDirectoryClient;
    
    setAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH, 0);
    setAttribute(WarpScriptStack.ATTRIBUTE_JSON_STRICT, false);
    setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT, new AtomicLong(0L));
    setAttribute(WarpScriptStack.ATTRIBUTE_GTS_COUNT, new AtomicLong(0L));
    setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_FETCH, Long.toString(WarpScriptStack.DEFAULT_FETCH_LIMIT))));
    setAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_GTS, Long.toString(WarpScriptStack.DEFAULT_GTS_LIMIT))));
    setAttribute(WarpScriptStack.ATTRIBUTE_ELAPSED, new ArrayList<Long>());
    setAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_LOOP_DURATION, Long.toString(WarpScriptStack.DEFAULT_MAX_LOOP_DURATION))));
    setAttribute(WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_RECURSION, Long.toString(WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_OPS, Long.toString(WarpScriptStack.DEFAULT_MAX_OPS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_SYMBOLS, Integer.toString(WarpScriptStack.DEFAULT_MAX_SYMBOLS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_DEPTH, Integer.toString(WarpScriptStack.DEFAULT_MAX_DEPTH))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_WEBCALLS, new AtomicLong(Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_WEBCALLS, Integer.toString(WarpScriptStack.DEFAULT_MAX_WEBCALLS)))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_BUCKETS, Integer.toString(WarpScriptStack.DEFAULT_MAX_BUCKETS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_PIXELS, Long.toString(WarpScriptStack.DEFAULT_MAX_PIXELS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_COUNT, new AtomicLong(0L));
    setAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_SIZE, new AtomicLong(0L));
    setAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_LIMIT, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_URLFETCH_LIMIT, Long.toString(WarpScriptStack.DEFAULT_URLFETCH_LIMIT))));
    setAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_MAXSIZE, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_URLFETCH_MAXSIZE, Long.toString(WarpScriptStack.DEFAULT_URLFETCH_MAXSIZE))));
    
    //
    // Set hard limits
    //
    
    setAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_LOOP_DURATION_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_LOOP_DURATION))));
    setAttribute(WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_RECURSION_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_DEPTH_HARD, Integer.toString(WarpScriptStack.DEFAULT_MAX_DEPTH))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_OPS_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_OPS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_SYMBOLS_HARD, Integer.toString(WarpScriptStack.DEFAULT_MAX_SYMBOLS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_BUCKETS_HARD, Integer.toString(WarpScriptStack.DEFAULT_MAX_BUCKETS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_PIXELS_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_PIXELS))));
    setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_FETCH_HARD, Long.toString(WarpScriptStack.DEFAULT_FETCH_LIMIT))));
    setAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_GTS_HARD, Long.toString(WarpScriptStack.DEFAULT_GTS_LIMIT))));
    setAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_LIMIT_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_URLFETCH_LIMIT_HARD, Long.toString(WarpScriptStack.DEFAULT_URLFETCH_LIMIT))));
    setAttribute(WarpScriptStack.ATTRIBUTE_URLFETCH_MAXSIZE_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_URLFETCH_MAXSIZE_HARD, Long.toString(WarpScriptStack.DEFAULT_URLFETCH_MAXSIZE))));

    //
    // Initialize counters
    //
    
    this.counters = new AtomicLong[1];
    
    for (int i = 0; i < this.counters.length; i++) {
      this.counters[i] = new AtomicLong(0L);
    }
  }
  
  @Override
  public int depth() {
    return list.size();
  }
  
  @Override
  public void reset(int depth) throws WarpScriptException {
    //
    // Remove the last element of the list until we reach 'depth'
    // We remove the last element to prevent having to shift remaining elements
    //
    
    while (list.size() > depth) {
      list.remove(list.size() - 1);
    }
  }
  
  @Override
  public void clear() {
    list.clear();
  }
  
  @Override
  public void drop() throws EmptyStackException {
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }
    
    list.remove(list.size() - 1);
  }

  @Override
  public void dropn() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();
    
    if (list.size() < n || n < 0) {
      throw new IndexOutOfBoundsException();
    }

    while (n > 0) {
      list.remove(list.size() - 1);
      n--;
    }
  }
  
  @Override
  public void dup() throws EmptyStackException {
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }
    
    Object element = list.get(list.size() - 1);
    list.add(element);
  }

  @Override
  public void dupn() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();
    
    if (list.size() < n || n < 0) {
      throw new IndexOutOfBoundsException();
    }
    
    int count = n;
    while (count > 0) {
      Object o = list.get(list.size() - 1 - (n - 1));
      list.add(o);
      count--;
    }
  }
  
  @Override
  public Object pop() throws EmptyStackException {
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }

    Object element = list.remove(list.size() - 1);
    
    return element;    
  }
  
  @Override
  public Object[] popn() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();
    
    if (list.size() < n || n < 0) {
      throw new IndexOutOfBoundsException();
    }
    
    Object[] objects = new Object[n];

    //
    // Remove objects from the end of the stack so the call to remove is blazing
    // fast.
    //
    
    for (int i = n - 1; i >= 0; i--) {
      objects[i] = list.remove(list.size() - 1);
    }
    
    return objects;
  }
  
  @Override
  public void push(Object o) throws WarpScriptException {
    
    if (list.size() > this.maxdepth) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_STACKDEPTH_EXCEEDED, Sensision.EMPTY_LABELS, 1);
      throw new WarpScriptException("Stack depth would exceed set limit of " + this.maxdepth);
    }
    
    list.add(o);
  }
  
  @Override
  public void swap() throws WarpScriptException, EmptyStackException, IndexOutOfBoundsException {
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }
    
    if (list.size() < 2) {
      throw new IndexOutOfBoundsException();
    }
    
    Object top = pop();
    Object top2 = pop();
    push(top);
    push(top2);
  }
  
  @Override
  public Object peek() throws EmptyStackException {
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }

    return list.get(list.size() - 1);
  }
  
  @Override
  public void rot() throws EmptyStackException, IndexOutOfBoundsException {
    
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }
    
    if (list.size() < 3) {
      throw new IndexOutOfBoundsException();
    }
    
    Object element = list.remove(list.size() - 1 - 2);
    list.add(element);
  }
  
  @Override
  public void roll() throws EmptyStackException, IndexOutOfBoundsException {    
    int n = getn();
    
    if (list.size() < n || n < 0) {
      throw new IndexOutOfBoundsException();
    }
    
    Object element = list.remove(list.size() - 1 - (n - 1));
    list.add(element);
  }
  
  @Override
  public Object peekn() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();
    
    if (list.size() < n - 1 || n < 0) {
      throw new IndexOutOfBoundsException();
    }
    
    return list.get(list.size() - 1 - n);
  }
  
  @Override
  public Object get(int n) throws WarpScriptException {
    if (list.size() < n - 1 || n < 0) {
      throw new WarpScriptException("Invalid level.");
    }
    
    return list.get(list.size() - 1 - n);
  }
  
  /**
   * Consume the top of the stack and interpret it as
   * an int number.
   * 
   * @return The int value of the top of the stack
   * 
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack is empty or its top is not a number. 
   */
  private int getn() throws EmptyStackException, IndexOutOfBoundsException {
    if (list.isEmpty()) {
      throw new EmptyStackException();
    }
    
    //
    // Extract the top of the stack and use it as 'N'
    //
    
    Object o = pop();
    
    if (! (o instanceof Number)) {
      throw new IndexOutOfBoundsException();
    }
    
    int n = ((Number) o).intValue();
    
    return n;
  }
  
  //private static final Pattern LONG_PATTERN = Pattern.compile("^[+-]?[0-9]+$");
  //private static final Pattern DOUBLE_PATTERN = Pattern.compile("^[+-]?[0-9]+\\.[0-9]+$");
    
  @Override
  public void execMulti(String script) throws WarpScriptException {
    BufferedReader br = new BufferedReader(new StringReader(script));
    
    int i = 1;
    
    try {
      while (true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        exec(line);
        i++;
      }      
      br.close();
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    } catch (Exception e) {
      throw new WarpScriptException("Line #" + i + ": " + e.getMessage());
    }
   
    //String[] lines = UnsafeString.split(script, '\n');
    //for (String line: lines) {
    //  exec(line);
    //}
  }
  
  @Override
  public void exec(String line) throws WarpScriptException {
    
    String rawline = line;
    
    try {
      if (this.recursionLevel.addAndGet(1) > this.maxrecurse) {
        throw new WarpScriptException("Maximum recursion level reached (" + this.recursionLevel.get() + ")");
      }
      
      String[] statements;
      
      line = line.trim();
                              
      //
      // Replace whistespaces in Strings with '%20'
      //
            
      line = UnsafeString.sanitizeStrings(line);
      
      if (-1 != UnsafeString.indexOf(line, ' ') && !inMultiline.get()) {
        //statements = line.split(" +");
        statements = UnsafeString.split(line, ' ');
      } else {
        statements = new String[1];
        statements[0] = inMultiline.get() ? rawline : line;
      }
      
      //
      // Report progress
      //
      
      progress();
      
      //
      // Loop over the statements
      //
      
      for (String stmt: statements) {

        //
        // Skip empty statements if we are not currently building a multiline
        //
        
        if (0 == stmt.length() && !inMultiline.get()) {
          continue;
        }
        
        //
        // Trim statement
        //
        
        if (!inMultiline.get()) {
          stmt = stmt.trim();
        }

        //
        // End execution on encountering a comment
        //
                
        if (!inMultiline.get() && stmt.length() > 0 && (stmt.charAt(0) == '#' || (stmt.charAt(0) == '/' && stmt.length() >= 2 && stmt.charAt(1) == '/'))) {
          // Skip comments and blank lines
          return;
        }

        if (WarpScriptStack.MULTILINE_END.equals(stmt)) {
          if (!inMultiline.get()) {
            throw new WarpScriptException("Not inside a multiline.");
          }
          inMultiline.set(false);
          if (null != secureScript) {
            secureScript.append(" ");
            secureScript.append("'");
            try {
              secureScript.append(URLEncoder.encode(multiline.toString(), "UTF-8"));
            } catch (UnsupportedEncodingException uee) {              
            }
            secureScript.append("'");
          } else {
            if (macros.isEmpty()) {
              this.push(multiline.toString());
            } else {
              macros.get(0).add(multiline.toString());
            }            
          }
          multiline.setLength(0);
          continue;
        } else if (inMultiline.get()) {
          if (multiline.length() > 0) {
            multiline.append("\n");            
          }
          multiline.append(stmt);
          continue;
        } else if (WarpScriptStack.COMMENT_END.equals(stmt)) {
          if (!inComment.get()) {
            throw new WarpScriptException("Not inside a comment.");
          }
          inComment.set(false);
          continue;
        } else if (inComment.get()) {
          continue;
        } else if (WarpScriptStack.COMMENT_START.equals(stmt)) {
          inComment.set(true);
          continue;
        } else if (WarpScriptStack.MULTILINE_START.equals(stmt)) {
          if (1 != statements.length) {
            throw new WarpScriptException("Can only start multiline strings by using " + WarpScriptStack.MULTILINE_START + " on a line by itself.");
          }
          inMultiline.set(true);
          multiline = new StringBuilder();
          continue;
        }
        
        incOps();

        if (WarpScriptStack.SECURE_SCRIPT_END.equals(stmt)) {
          if (null == secureScript) {
            throw new WarpScriptException("Not inside a secure script definition.");
          } else {
            this.push(secureScript.toString());
            new SECURE("SECURESCRIPT").apply(this);
            secureScript = null;
          }
        } else if (WarpScriptStack.SECURE_SCRIPT_START.equals(stmt)) {
          if (null == secureScript) {
            secureScript = new StringBuilder();
          } else {
            throw new WarpScriptException("Already inside a secure script definition.");
          }
        } else if (null != secureScript) {
          secureScript.append(" ");
          secureScript.append(stmt);
        } else if (WarpScriptStack.MACRO_END.equals(stmt)) {
          if (macros.isEmpty()) {
            throw new WarpScriptException("Not inside a macro definition.");
          } else {
            Macro lastmacro = macros.remove(0);
            
            if (macros.isEmpty()) {
              this.push(lastmacro);
            } else {
              // Add the macro to the outer macro
              macros.get(0).add(lastmacro);
            }
          }
        } else if (WarpScriptStack.MACRO_START.equals(stmt)) {
          //
          // Create holder for current macro
          //
          
          macros.add(0, new Macro());
        } else if ((stmt.charAt(0) == '\'' && stmt.charAt(stmt.length() - 1) == '\'')
            || (stmt.charAt(0) == '\"' && stmt.charAt(stmt.length() - 1) == '\"')) {
          //
          // Push Strings onto the stack
          //
          
          try {
            String str = URLDecoder.decode(stmt.substring(1, stmt.length() - 1), "UTF-8");
            if (macros.isEmpty()) {
              push(str);
            } else {
              macros.get(0).add(str);
            }
          } catch (UnsupportedEncodingException uee) {
            // Cannot happen...
            throw new WarpScriptException(uee);
          }
        } else if (stmt.length() > 3 && stmt.charAt(1) == 'x' && stmt.charAt(0) == '0') {
          long hexl = Long.parseLong(stmt.substring(2), 16);
          if (macros.isEmpty()) {
            push(hexl);
          } else {
            macros.get(0).add(hexl);
          }
        } else if (stmt.length() > 3 && stmt.charAt(1) == 'b' && stmt.charAt(0) == '0') {
          long binl = Long.parseLong(stmt.substring(2), 2);
          if (macros.isEmpty()) {
            push(binl);
          } else {
            macros.get(0).add(binl);
          }
        } else if (UnsafeString.isLong(stmt)) {
          //
          // Push longs onto the stack
          //
          
          if (macros.isEmpty()) {
            push(Long.valueOf(stmt));
          } else {
            macros.get(0).add(Long.valueOf(stmt));
          }
        } else if (UnsafeString.isDouble(stmt)) {
          //
          // Push doubles onto the stack
          //
          if (macros.isEmpty()) {
            push(Double.valueOf(stmt));
          } else {
            macros.get(0).add(Double.valueOf(stmt));
          }
        } else if (stmt.equalsIgnoreCase("T")
                   || stmt.equalsIgnoreCase("F")
                   || stmt.equalsIgnoreCase("true")
                   || stmt.equalsIgnoreCase("false")) {
          //
          // Push booleans onto the stack
          //
          if (stmt.startsWith("T") || stmt.startsWith("t")) {
            if (macros.isEmpty()) {
              push(true);
            } else {
              macros.get(0).add(true);
            }
          } else {
            if (macros.isEmpty()) {
              push(false);
            } else {
              macros.get(0).add(false);
            }
          }
        } else if (stmt.startsWith("$")) {
          if (macros.isEmpty()) {
            //
            // This is a deferred variable dereference
            //
            Object o = load(stmt.substring(1));
            
            if (null == o) {
              throw new WarpScriptException("Unknown symbol '" + stmt.substring(1) + "'");
            }
            
            push(o);
          } else {
            macros.get(0).add(stmt.substring(1));
            macros.get(0).add(WarpScriptLib.getFunction(WarpScriptLib.LOAD));
          }
        } else if (stmt.startsWith("!$")) {
          //
          // This is an immediate variable dereference
          //
          Object o = load(stmt.substring(2));
          
          if (null == o) {
            throw new WarpScriptException("Unknown symbol '" + stmt.substring(2) + "'");
          }

          if (macros.isEmpty()) {
            push(o);
          } else {
            macros.get(0).add(o);
          }
        } else if (stmt.startsWith("@")) {          
          if (macros.isEmpty()) {
            //
            // This is a macro dereference
            //
            
            String symbol = stmt.substring(1);

            run(symbol);
          } else {
            macros.get(0).add(stmt.substring(1));
            macros.get(0).add(WarpScriptLib.getFunction(WarpScriptLib.RUN));
          }          
        } else {
          //
          // This is a function call
          //

          Object func = null;
          
          //
          // Check Einstein functions
          //

          func = null != func ? func : defined.get(stmt);
          func = null != func ? func : WarpScriptLib.getFunction(stmt);
          func = null != func ? func : JavaLibrary.getFunction(stmt);

          if (null == func) {
            throw new WarpScriptException("Unknown function '" + stmt + "'");
          }

          Map<String,String> labels = new HashMap<String,String>();
          labels.put(SensisionConstants.SENSISION_LABEL_FUNCTION, stmt);
          
          long nano = System.nanoTime();
          
          try {
            if (func instanceof WarpScriptStackFunction && macros.isEmpty()) {
              //
              // Function is an EinsteinStackFunction, call it on this stack
              //
              
              WarpScriptStackFunction esf = (WarpScriptStackFunction) func;

              esf.apply(this);
            } else {
              //
              // Push any other type of function onto the stack
              //
              if (macros.isEmpty()) {
                push(func);
              } else {
                macros.get(0).add(func);
              }
            }          
          } finally {
            Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_FUNCTION_COUNT, labels, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_FUNCTION_TIME_US, labels, (System.nanoTime() - nano) / 1000L);
          }
        }
      }
      
      return;      
    } finally {
      this.recursionLevel.addAndGet(-1);
    }
  }
  
  @Override
  public void exec(Macro macro) throws WarpScriptException {

    // We increment op count for the macro itself. We'll increment
    // for each statement of the macro inside the loop
    incOps();

    boolean secure = Boolean.TRUE.equals(this.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO));
    
    this.setAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO, macro.isSecure());
    
    try {
      
      if (this.recursionLevel.addAndGet(1) > this.maxrecurse) {
        throw new WarpScriptException("Maximum recursion level reached (" + this.recursionLevel.get() + ")");
      }

      for (int i = 0; i < macro.size(); i++) {
        // Notify progress
        progress();
        
        Object stmt = macro.get(i);
        
        incOps();
        
        if (stmt instanceof WarpScriptStackFunction) {
          WarpScriptStackFunction esf = (WarpScriptStackFunction) stmt;
          
          // FIXME(hbs): we do not count the number of calls or the time spent in individual functions
          // called from macros - We could correct that but it would have a great impact on performance
          // as we would be calling sensision update potentially several billion times per script
          
          //long nano = System.nanoTime();

          esf.apply(this);
          
          //Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_FUNCTION_COUNT, esf.getSensisionLabels(), 1);
          //Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_FUNCTION_TIME_US, esf.getSensisionLabels(), (System.nanoTime() - nano) / 1000L);          
        } else {
          push(stmt);
        }
      }     
    } catch (WarpScriptReturnException ere) {
      if (this.getCounter(WarpScriptStack.COUNTER_RETURN_DEPTH).decrementAndGet() > 0) {
        throw ere;
      }
    } finally {
      this.setAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO, secure);
      this.recursionLevel.addAndGet(-1);
    }
  }
  
  @Override
  public void exec(WarpScriptJavaFunction function) throws WarpScriptException {
    //
    // Check if we can execute the UDF. We enclose this call in a try/catch since we could get weird errors
    // when the wrong classes were used for EinsteinJavaFunction. Better err on the side of safety here.
    //
    
    try {
      if (function.isProtected() && !Boolean.TRUE.equals(this.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO))) {
        throw new WarpScriptException("UDF is protected.");
      }
    } catch (Throwable t) {
      throw new WarpScriptException(t);
    }
    
    //
    // Determine the number of levels of the stack the function needs
    //
    
    int levels = function.argDepth();
    
    if (this.list.size() < levels) {
      throw new WarpScriptException("Stack does not contain sufficient elements.");
    }
    
    // Build the list of objects, the top of the stack being the first
    List<Object> args = new ArrayList<Object>(levels);

    if (function instanceof WarpScriptRawJavaFunction) {
      args.add(this);
    } else {
      for (int i = 0; i < levels; i++) {
        args.add(StackUtils.toSDKObject(this.pop()));
      }      
    }
    
    try {
      // Apply the function
      List<Object> results = function.apply(args);
      
      if (!(function instanceof WarpScriptRawJavaFunction)) {
        // Push the results onto the stack
        for (Object result: results) {
          this.push(StackUtils.fromSDKObject(result));
        }        
      }
    } catch (WarpScriptJavaFunctionException ejfe) {
      throw new WarpScriptException(ejfe);
    }
  }
  
  @Override
  public void run(String symbol) throws WarpScriptException {
    //
    // Look up the macro in the local symbol table
    //
    
    Object macro = load(symbol);
    
    //
    // Now attempt to look it up in the various repos
    //
        
    if (null == macro) {
      macro = WarpScriptMacroRepository.find(symbol);
    }
    
    if (null == macro) {
      macro = WarpScriptMacroLibrary.find(symbol);
    }

    if (null == macro) {
      throw new WarpScriptException("Unknown macro '" + symbol + "'");
    }
    
    if (!(macro instanceof Macro)) {
      throw new WarpScriptException("'" + symbol + "' is not a macro.");
    }
    
    //
    // Execute macro
    //
    
    exec((Macro) macro);    
  }
  
  @Override
  public String dump(int n) {
    StringBuilder sb = new StringBuilder();
    
    for (int i = n - 1; i >= 0; i--) {
      if (i < this.list.size()) {
        sb.append(i + 1);
        sb.append(": ");
        Object elt = this.list.get(list.size() - 1 - i);
        
        if (elt instanceof Object[]) {
          sb.append(Arrays.toString((Object[]) elt));
        } else {
          sb.append(elt);
        }

        sb.append("\n");
      }
    }
    
    return sb.toString();
  }
  
  @Override
  public void pick() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();
    
    if (list.size() < n || n < 0) {
      throw new IndexOutOfBoundsException();
    }
      
    list.add(list.get(list.size() - 1 - (n - 1)));
  }
  
  @Override
  public void rolld() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();
    
    if (list.size() < n || n < 0) {
      throw new IndexOutOfBoundsException();
    }
      
    Object element = list.remove(list.size() - 1);
    list.add(list.size() - (n - 1), element);
  }
  
  @Override
  public Object load(String symbol) {
    return this.symbolTable.get(symbol);
  }
  
  @Override
  public void store(String symbol, Object value) throws WarpScriptException {
    
    if (this.symbolTable.size() >= this.maxsymbols) {
      throw new WarpScriptException("Symbol table has reached its maximum number of entries: " + this.maxsymbols);
    }
    
    this.symbolTable.put(symbol, value);
  }
  
  @Override
  public void forget(String symbol) {
    if (null == symbol) {
      this.symbolTable.clear();
    } else {
      this.symbolTable.remove(symbol);
    }
  }
  
  @Override
  public Map<String,Object> getSymbolTable() {
    return this.symbolTable;
  }
  
  @Override
  public Map<String, WarpScriptStackFunction> getDefined() {
    return this.defined;
  }
  
  @Override
  public String getUUID() {
    return this.uuid;
  }
  
  @Override
  public Object setAttribute(String key, Object value) {
    Object currentValue = this.attributes.get(key);
    
    if (null == value) {
      this.attributes.remove(key);
    } else {
      this.attributes.put(key, value);
    }
    
    if (WarpScriptStack.ATTRIBUTE_MAX_DEPTH.equals(key)) {
      this.maxdepth = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_MAX_OPS.equals(key)) {
      this.maxops = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH.equals(key)) {
      this.maxrecurse = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS.equals(key)) {
      this.maxsymbols = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_OPS.equals(key)) {
      this.currentops = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE.equals(key)) {
      this.progressable = (Progressable) value;
    }
    
    return currentValue;
  }
  
  @Override
  public Object getAttribute(String key) {
    // Manage the number of ops in a special way
    if (WarpScriptStack.ATTRIBUTE_OPS.equals(key)) {
      return this.currentops;
    } else {
      return this.attributes.get(key);
    }
  }
  
  /**
   * Increment the operation count and check for limit.
   * We do not need to be synchronized since the stack is
   * called by a single thread
   * 
   * @throws WarpScriptException
   */
  private final void incOps() throws WarpScriptException {
    this.currentops++;
    
    if (this.currentops > this.maxops) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_EINSTEIN_OPSCOUNT_EXCEEDED, Sensision.EMPTY_LABELS, 1);
      throw new WarpScriptException("Operation count (" + this.currentops + ") exceeded maximum of " + this.maxops);
    }
  }
  
  @Override
  public AtomicLong getCounter(int i) throws WarpScriptException {
    if (i >= 0 && i <= this.counters.length) {
      return this.counters[i];
    }
    throw new WarpScriptException("Invalid counter.");
  }
  
  @Override
  public void progress() {
    if (null != this.progressable) {
      this.progressable.progress();
    }
  }
  
  @Override
  public boolean isAuthenticated() {
    return null != this.getAttribute(ATTRIBUTE_TOKEN);
  }
  
  @Override
  public void checkBalanced() throws WarpScriptException {
    if (inMultiline.get()) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.MULTILINE_START + " construct.");
    }
    if (inComment.get()) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.COMMENT_START + " construct.");
    }
    if (null != secureScript) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.SECURE_SCRIPT_START + " construct.");
    }
    if (!macros.isEmpty()) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.MACRO_START + " construct.");
    }
  }
  
  /**
   * (re)define a statement
   * 
   * If 'macro' is null, clear the (re)definition of 'stmt'
   */
  @Override
  public void define(String stmt, final Macro macro) {
    if (null == macro) {
      this.defined.remove(stmt);
    } else {
      // Wrap the macro into a function
      WarpScriptStackFunction func = new WarpScriptStackFunction() {        
        @Override
        public Object apply(WarpScriptStack stack) throws WarpScriptException {
          stack.exec(macro);
          return stack;
        }
      };
      this.defined.put(stmt, func);
    }
  }
  
  @Override
  public void save() throws WarpScriptException {
    //
    // Create a new StackContext
    //
    
    StackContext context = new StackContext();
    
    //
    // Copy symbol table
    //
    
    context.symbolTable = new HashMap<String, Object>();    
    context.symbolTable.putAll(this.symbolTable);
    
    //
    // Copy redefined functions
    //
    
    context.defined = new HashMap<String, WarpScriptStackFunction>();
    context.defined.putAll(this.defined);
    
    //
    // Push context onto the stack
    //
    
    this.push(context);    
  }
  
  @Override
  public void restore() throws WarpScriptException {
    //
    // Retrieve the object on top of the stack
    //
    
    Object top = this.pop();
    
    if (!(top instanceof StackContext)) {
      throw new WarpScriptException("Invalid stack context.");
    }
    
    StackContext context = (StackContext) top;
    
    //
    // Restore symbol table
    //
    
    this.symbolTable.clear();
    
    if (null != context.symbolTable) {
      this.symbolTable.putAll(context.symbolTable);
    }
    
    //
    // Restore redefined functions
    //
    
    this.defined.clear();
    if (null != context.defined) {
      this.defined.putAll(context.defined);
    }
  }  
}
