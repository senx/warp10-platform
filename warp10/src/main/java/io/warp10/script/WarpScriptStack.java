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

import io.warp10.continuum.geo.GeoDirectoryClient;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.functions.SNAPSHOT;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;
import io.warp10.warp.sdk.WarpScriptJavaFunction;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Einstein Geo Time Serie manipulation environment
 * usually uses a stack to operate.
 * 
 * There may be multiple implementations of stacks that
 * Einstein can use, including some that persist to a
 * cache or that may spill to disk.
 * 
 * All those implementations MUST implement this interface.
 *
 */
public interface WarpScriptStack {
  
  public static final int DEFAULT_MAX_RECURSION_LEVEL = 16;
  public static final long DEFAULT_FETCH_LIMIT = 100000L;
  public static final long DEFAULT_GTS_LIMIT = 100000L;
  public static final long DEFAULT_MAX_OPS = 1000L;
  public static final int DEFAULT_MAX_BUCKETS = 1000000;
  public static final int DEFAULT_MAX_GEOCELLS = 10000;
  public static final int DEFAULT_MAX_DEPTH = 1000;
  public static final long DEFAULT_MAX_LOOP_DURATION = 5000L;
  public static final int DEFAULT_MAX_SYMBOLS = 64;
  public static final int DEFAULT_MAX_WEBCALLS = 4;
  public static final long DEFAULT_MAX_PIXELS = 1000000L;
  public static final long DEFAULT_URLFETCH_LIMIT = 64;
  public static final long DEFAULT_URLFETCH_MAXSIZE = 1000000L;
  
  public static final String MACRO_START = "<%";
  public static final String MACRO_END = "%>";

  public static final String COMMENT_START = "/*";
  public static final String COMMENT_END = "*/";
  
  public static final String MULTILINE_START = "<'";
  public static final String MULTILINE_END = "'>";
  
  public static final String SECURE_SCRIPT_START = "<S";
  public static final String SECURE_SCRIPT_END = "S>";
  
  public static final String TOP_LEVEL_SECTION = "[TOP]";
  
  /**
   * Prefix for traceing push/pop
   */
  public static final String ATTRIBUTE_TRACE_PREFIX = "trace.prefix";
  
  /**
   * Name of current code section, null is unnamed
   */
  public static final String ATTRIBUTE_SECTION_NAME = "section.name";
  
  /**
   * Flag indicating whether or not the stack is currently in documentation mode
   */
  public static final String ATTRIBUTE_DOCMODE = "docmode";
  
  /**
   * Flag indicating whether or not the stack is currently in signature mode
   */
  public static final String ATTRIBUTE_SIGMODE = "sigmode";
  
  /**
   * Debug depth of the stack. This is the number
   * of elements to return when an error occurs.
   */
  public static final String ATTRIBUTE_DEBUG_DEPTH = "debug.depth";
  
  /**
   * Is the stack configured to output strict JSON (i.e with no NaN/Infinity)?
   */
  public static final String ATTRIBUTE_JSON_STRICT = "json.strict";
  
  /**
   * Maximum number of datapoints that can be fetched in a session
   */
  public static final String ATTRIBUTE_FETCH_LIMIT = "fetch.limit";
  public static final String ATTRIBUTE_FETCH_LIMIT_HARD = "fetch.limit.hard";

  /**
   * Maximum number of GTS which can be retrieved from directory in a session
   */
  public static final String ATTRIBUTE_GTS_LIMIT = "gts.limit";
  public static final String ATTRIBUTE_GTS_LIMIT_HARD = "gts.limit.hard";
  
  /**
   * Number of datapoints fetched so far in the session
   */
  public static final String ATTRIBUTE_FETCH_COUNT = "fetch.count";

  /**
   * Number of GTS retrieved so far in the session
   */
  public static final String ATTRIBUTE_GTS_COUNT = "gts.count";

  /**
   * Maximum number of calls to URLFETCH in a session
   */
  public static final String ATTRIBUTE_URLFETCH_LIMIT = "urlfetch.limit";
  public static final String ATTRIBUTE_URLFETCH_LIMIT_HARD = "urlfetch.limit.hard";

  /**
   * Number of calls to URLFETCH so far in the sessions
   */
  public static final String ATTRIBUTE_URLFETCH_COUNT = "urlfetch.count";
  
  /**
   * Maximum size of content retrieved via calls to URLFETCH in a session
   */
  public static final String ATTRIBUTE_URLFETCH_MAXSIZE = "urlfetch.maxsize";
  public static final String ATTRIBUTE_URLFETCH_MAXSIZE_HARD = "urlfetch.maxsize.hard";

  /**
   * Current  URLFETCH so far in the sessions
   */
  public static final String ATTRIBUTE_URLFETCH_SIZE = "urlfetch.size";
  
  /**
   * List of elapsed times (in ns) per line
   */
  public static final String ATTRIBUTE_ELAPSED = "elapsed";
  
  /**
   * Flag indicating whether or not to track elapsed times per script line
   */
  public static final String ATTRIBUTE_TIMINGS = "timings";
  
  /**
   * Maximum duration of loops in ms and its hard limit
   */
  public static final String ATTRIBUTE_LOOP_MAXDURATION = "loop.maxduration";
  public static final String ATTRIBUTE_LOOP_MAXDURATION_HARD = "loop.maxduration.hard";
  
  /**
   * Maximum recursion depth
   */
  public static final String ATTRIBUTE_RECURSION_MAXDEPTH = "recursion.maxdepth";
  public static final String ATTRIBUTE_RECURSION_MAXDEPTH_HARD = "recursion.maxdepth.hard";
  
  /**
   * Maximum depth of the stack
   */
  public static final String ATTRIBUTE_MAX_DEPTH = "stack.maxdepth";
  public static final String ATTRIBUTE_MAX_DEPTH_HARD = "stack.maxdepth.hard";

  /**
   * Maximum number of operations for the stack
   */
  public static final String ATTRIBUTE_MAX_OPS = "stack.maxops";
  public static final String ATTRIBUTE_MAX_OPS_HARD = "stack.maxops.hard";

  /**
   * Maximum number of pixels for images created on the stack
   */
  public static final String ATTRIBUTE_MAX_PIXELS = "stack.maxpixels";
  public static final String ATTRIBUTE_MAX_PIXELS_HARD = "stack.maxpixels.hard";
  
  /**
   * Maximum number of buckets in bucketized GTS
   */
  public static final String ATTRIBUTE_MAX_BUCKETS = "stack.maxbuckets";
  public static final String ATTRIBUTE_MAX_BUCKETS_HARD = "stack.maxbuckets.hard";
  
  /**
   * Maximum number of cells if GeoXPShapes   
   */
  public static final String ATTRIBUTE_MAX_GEOCELLS = "stack.maxgeocells";
  public static final String ATTRIBUTE_MAX_GEOCELLS_HARD = "stack.maxgeocells.hard";
  
  /**
   * Current number of operations performed on this stack
   */
  public static final String ATTRIBUTE_OPS = "stack.ops";
  
  /**
   * Maximum number of symbols for the stack
   */
  public static final String ATTRIBUTE_MAX_SYMBOLS = "stack.symbols";
  public static final String ATTRIBUTE_MAX_SYMBOLS_HARD = "stack.symbols.hard";

  /**
   * Key for securing scripts
   */
  public static final String ATTRIBUTE_SECURE_KEY = "secure.key";
  
  /**
   * Flag indicating whether or not redefined functions are allowed
   */
  public static final String ATTRIBUTE_ALLOW_REDEFINED = "allow.redefined";
  
  /**
   * Key for storing an instance of Hadoop's Progressable to report progress to the Hadoop framework
   */
  public static final String ATTRIBUTE_HADOOP_PROGRESSABLE = "hadoop.progressable";
  
  /**
   * Maximum number of WEBCALL invocations per script run
   */
  public static final String ATTRIBUTE_MAX_WEBCALLS = "stack.maxwebcalls";
  
  /**
   * Token which was used to authenticate the stack, checked by some protected ops
   */
  public static final String ATTRIBUTE_TOKEN = "stack.token";
  
  /**
   * Flag indicating if we are currently in a secure macro execution
   */
  public static final String ATTRIBUTE_IN_SECURE_MACRO = "in.secure.macro";
  
  /**
   * List of symbols to export upon script termination as a map of symbol name
   * to symbol value pushed onto the stack.
   */
  public static final String ATTRIBUTE_EXPORTED_SYMBOLS = "exported.symbols";
  
  /**
   * Map of headers to return with the response
   */
  public static final String ATTRIBUTE_HEADERS = "response.headers";
  
  /**
   * Last error encountered in a TRY block
   */
  public static final String ATTRIBUTE_LAST_ERROR = "last.error";
  
  /**
   * Index of RETURN_DEPTH counter
   */
  public static final int COUNTER_RETURN_DEPTH = 0;
  
  public static class StackContext {}
  
  public static class Mark {}
  
  public static class Macro implements Snapshotable {
    
    /**
     * Flag indicating whether a macro is secure (its content cannot be displayed) or not
     */
    private boolean secure = false;
    
    private long fingerprint;
    
    private ArrayList<Object> statements = new ArrayList<Object>();
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      
      sb.append(MACRO_START);
      sb.append(" ");

      if (!secure) {
        for (Object o: this.statements()) {
          sb.append(StackUtils.toString(o));
          sb.append(" ");        
        }
      } else {
        sb.append(WarpScriptStack.COMMENT_START);
        sb.append(" Secure Macro ");
        sb.append(WarpScriptStack.COMMENT_END);
        sb.append(" ");
      }
      
      sb.append(MACRO_END);
      
      return sb.toString();
    }
    
    public void add(Object o) {
      this.statements().add(o);
    }
    
    public Object get(int idx) {
      return this.statements().get(idx);
    }
    
    public int size() {
      return this.statements().size();
    }
    
    public List<Object> statements() {
      return this.statements;
    }
    
    public void addAll(Macro macro) {
      this.statements().addAll(macro.statements());
    }
    
    public void setSecure(boolean secure) {
      this.secure = secure;
    }
    
    public boolean isSecure() {
      return this.secure;
    }
    
    public long getFingerprint() {
      return this.fingerprint;
    }
    
    public void setFingerprint(long fingerprint) {
      this.fingerprint = fingerprint;
    }
    
    @Override
    public String snapshot() {
      StringBuilder sb = new StringBuilder();
      
      sb.append(MACRO_START);
      sb.append(" ");

      if (!secure) {
        for (Object o: this.statements()) {
          try {
            SNAPSHOT.addElement(sb, o);
          } catch (WarpScriptException wse) {
            sb.append(WarpScriptStack.COMMENT_START);
            sb.append(" Error while snapshoting element of type '" + o.getClass() + "' ");
            sb.append(WarpScriptStack.COMMENT_END);
          }
          sb.append(" ");        
        }
      } else {
        sb.append(WarpScriptStack.COMMENT_START);
        sb.append(" Secure Macro ");
        sb.append(WarpScriptStack.COMMENT_END);
        sb.append(" ");
      }
      
      sb.append(MACRO_END);
      
      return sb.toString();
    }
  }
  
  /**
   * Retrieve the StoreClient instance associated with this stack.
   * @return 
   */
  public StoreClient getStoreClient();

  /**
   * Retrieve the DirectoryClient instance associated with this stack
   * @return
   */
  public DirectoryClient getDirectoryClient();
  
  /**
   * Retrieve the GeoDirectoryClient instance associated with this stack
   * @return
   */
  public GeoDirectoryClient getGeoDirectoryClient();
  
  /**
   * Push an object onto the stack
   * 
   * @param o Object to push onto the stack
   */
  public void push(Object o) throws WarpScriptException;
  
  /**
   * Remove and return the object on top of the stack.
   * 
   * @return The object on top of the stack
   * 
   * @throws EmptyStackException if the stack is empty.
   */
  public Object pop() throws EmptyStackException;
  
  /**
   * Remove and return 'N' objects from the top of the
   * stack.
   * 
   * 'N' is consumed at the top of the stack prior to
   * removing and returning the objects.
   * 
   * 
   * @return An array of 'N' objects, the first being the deepest.
   *  
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException If 'N' is not present or if
   *         'N' is invalid or if the stack is not deep enough.
   */
  public Object[] popn() throws WarpScriptException;
  
  /**
   * Return the object on top of the stack without removing
   * it from the stack.
   * 
   * @return The object on top of the stack
   * 
   * @throws EmptyStackException if the stack is empty.
   */
  public Object peek() throws EmptyStackException;
  
  /**
   * Return the object at 'distance' from the top of the stack.
   * The 'distance' is on top of the stack and is consumed by 'peekn'.
   * 
   * The object on top the stack is at distance 0.
   * 
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundException if no valid 'distance' is on top of the stack or if the
   *         requested distance is passed the bottom of the stack. 
   */
  public Object peekn() throws WarpScriptException;
  
  /**
   * Return the depth (i.e. number of objects) of the stack
   * 
   * @return The depth of the stack
   */
  public int depth();
  
  /**
   * Reset the stack to the given depth
   */
  public void reset(int depth) throws WarpScriptException;
  
  /**
   * Swap the top 2 objects of the stack.
   * 
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack is empty or
   *         contains a single element.
   */
  public void swap() throws WarpScriptException;
  
  /**
   * Rotate the top 3 objects of the stack, pushing
   * the top of the stack down
   * 
   *    D                     D
   *    C           ->        B
   *    B                     A
   *    A (top)               C (top)
   *    
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack contains less than 3 objects
   */
  public void rot() throws WarpScriptException;
  
  /**
   * Rotate up the top 'N' objects of the stack.
   * 'N' is on top of the stack and is consumed by 'roll'.
   * 
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if 'N' is not present on top of the stack,
   *         is not a number or if the stack does not have enough elements for the
   *         operation.
   */
  public void roll() throws WarpScriptException;
  
  /**
   * Rotate down the top 'N' objects of the stack.
   * 'N' is on the top of the stack and is consumed by 'rolld'.
   * 
   * @throws EmptyStackException if the stack is empty
   * @throws IndexOutOfBoundsException if 'N' is not present on top of the stack,
   *         is not a number or if the stack does not have enough elements for the
   *         operation.
   */
  public void rolld() throws WarpScriptException;
  
  /**
   * Copy the object at level 'N' on top of the stack.
   * 'N' is on top of the stack and is consumed by the call to 'pick'.
   * 
   * @throws EmptyStackException
   * @throws IndexOutOfBoundsException
   */
  public void pick() throws WarpScriptException;
  
  /**
   * Remove the top of the stack.
   * 
   * @throws EmptyStackException If the stack is empty.
   */
  public void drop() throws WarpScriptException;
  
  /**
   * Remove the top 'N' objects of the stack.
   * 
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException If 'N' is not present on the top of the stack,
   *         is not a number
   *         or if the stack has fewer than 'N' objects after consuming 'N'.
   */
  public void dropn() throws WarpScriptException;
  
  /**
   * Duplicate the object on top of the stack.
   * 
   * @throws EmptyStackException if the stack is empty.
   */
  public void dup() throws WarpScriptException;
  
  /**
   * Duplicate the top 'N' objects of the stack.
   * 'N' is consumed at the top of the stack first.
   * 
   * @throws EmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack contains less than 'N' objects.
   */
  public void dupn() throws WarpScriptException;

  /**
   * Return the object at level 'level' on the stack.
   * The top of the stack is level 0
   * 
   * @param level Level of the object to return
   * @return The object found at 'level'
   * @throws WarpScriptException if the stack contains less than 'level' levels
   */
  public Object get(int level) throws WarpScriptException;
  
  public void execMulti(String script) throws WarpScriptException;
  
  /**
   * Execute a serie of statements against the stack.
   * 
   * @param line String containing a space separated list of statements to execute
   * @return
   * @throws Exception
   */
  public void exec(String line) throws WarpScriptException;
  
  /**
   * Empty the stack
   * 
   */
  public void clear();
  
  /**
   * Execute a macro against the stack.
   * 
   * @param macro Macro instance to execute
   * @return
   * @throws WarpScriptException
   */
  public void exec(Macro macro) throws WarpScriptException;

  /**
   * Execute an EinsteinJavaFunction against the stack
   * 
   * @param function
   * @throws WarpScriptException
   */
  public void exec(WarpScriptJavaFunction function) throws WarpScriptException;

  /**
   * Find a macro by name
   * 
   * @param macroName Name of macro to find
   * @throws WarpScriptException if macro is not found
   */
  public Macro find(String macroName) throws WarpScriptException;
  
  /**
   * Execute a macro known by name.

   * @param macroName
   * @throws WarpScriptException
   */
  public void run(String macroName) throws WarpScriptException;
    
  /**
   * Produces a String representation of the top 'n' levels of the stack
   * @param n Number of stack levels to display at most
   * @return
   */
  public String dump(int n);
  
  /**
   * Return the content associated with the given symbol.
   * 
   * @param symbol Name of symbol to retrieve
   * 
   * @return The content associated with 'symbol' or null if 'symbol' is not known.
   */
  public Object load(String symbol);
  
  /**
   * Store the given object under 'symbol'.
   * 
   * @param symbol Name under which to store a value.
   * @param value Value to store.
   */
  public void store(String symbol, Object value) throws WarpScriptException;
  
  /**
   * Forget the given symbol
   * 
   * @param symbol Name of the symbol to forget.
   */
  public void forget(String symbol);
  
  /**
   * Return the current symbol table.
   * 
   * @return
   */
  public Map<String,Object> getSymbolTable();
  
  /**
   * Return the current map of redefined functions
   * @return
   */
  public Map<String,WarpScriptStackFunction> getDefined();
  
  /**
   * Return a UUID for the instance of EinsteinStack
   * @return
   */
  public String getUUID();
  
  /**
   * Set a stack attribute.
   * 
   * @param key Key under which the attribute should be stored.
   * @param value Value of the attribute. If null, remove the attribute.
   * @return The previous value of the attribute or null if it was not set.
   */
  public Object setAttribute(String key, Object value);
  
  /**
   * Return the value of an attribute.
   * 
   * @param key Name of the attribute to retrieve.
   * 
   * @return The value store unded 'key' or null
   */
  public Object getAttribute(String key);
  
  /**
   * Return the ith counter associated with the stack
   * @param i
   * @return
   */
  public AtomicLong getCounter(int i) throws WarpScriptException;
  
  /**
   * Returns a boolean indicating whether or not the stack is authenticated.
   *
   * @return The authentication status of the stack
   */
  public boolean isAuthenticated();
  
  /**
   * Perform a final check to ensure balancing constructs are balanced.
   * 
   * @throws WarpScriptException if the stack is currently unbalanced.
   */
  public void checkBalanced() throws WarpScriptException;
  
  /**
   * (re)define 'stmt' as a valid statement executing 'macro'
   * This allows for the overriding of built-in statements
   * 
   * @param stmt
   * @param macro
   */
  public void define(String stmt, Macro macro);
  
  /**
   * Push the current stack context (symbols + redefined statements) onto the stack.
   * 
   */
  public void save() throws WarpScriptException;
  
  /**
   * Restore the stack context from that on top of the stack
   */
  public void restore() throws WarpScriptException;  
}
