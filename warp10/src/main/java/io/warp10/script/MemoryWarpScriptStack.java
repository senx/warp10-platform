//
//   Copyright 2020-2023  SenX S.A.S.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.warp10.script.functions.MSGFAIL;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Progressable;

import io.warp10.WarpConfig;
import io.warp10.WarpURLDecoder;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.functions.SECURE;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.MacroResolver;

public class MemoryWarpScriptStack implements WarpScriptStack, Progressable {

  private static final Properties DEFAULT_PROPERTIES;

  static {
    DEFAULT_PROPERTIES = WarpConfig.getProperties();
  }

  /**
   * Depth of the macros after openMacro was called.
   */
  private int forcedMacro = 0;

  /**
   * Should we update per function metrics
   */
  private boolean functionMetrics = true;

  private Signal signal = null;
  private boolean signaled = false;

  private final boolean allowLooseBlockComments;

  private AtomicLong[] counters;

  private final Object[] registers;

  /**
   * Default maximum depth of the stack
   */
  private int maxdepth = 0;

  /**
   * Maximum number of operations for this stack
   */
  private long maxops = 0L;

  /**
   * Maximum number of entries in the symbol table
   */
  private int maxsymbols = 0;

  /**
   * Maximum recursion level
   */
  private long maxrecurse = 0L;

  /**
   * Current number of operations on this stack.
   * FIXME(hbs): use an AtomicLong is exec is to be called in an MT environment
   */
  private long currentops = 0L;

  private String sectionName = null;

  private String macroName = null;

  /**
   * Are we currently in a secure macro?
   */
  private boolean inSecureMacro = false;

  /**
   * Current number of visible (not hidden) elements
   */
  private int size = 0;

  /**
   * Offset, 0 means no part of the stack is hidden, > 0 means 'offset' elements are hidden
   */
  private int offset = 0;

  private Object[] elements = new Object[32];

  private final Map<String,Object> symbolTable = new HashMap<String,Object>();

  /**
   * Map of stack attributes. This is used to store various values such as
   * limits or formats.
   */
  private final Map<String,Object> attributes = new HashMap<String,Object>();

  private StoreClient storeClient;

  private DirectoryClient directoryClient;

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

  private boolean auditMode = false;
  /**
   * (re)defined functions
   */
  private Map<String,WarpScriptStackFunction> defined = new HashMap<String, WarpScriptStackFunction>();

  /**
   * Optional Hadoop progressable
   */
  private Progressable progressable = null;

  private Properties properties;

  private final boolean unshadow;

  private final long creationTime = System.currentTimeMillis();

  public static class StackContext extends WarpScriptStack.StackContext {
    public Map<String, Object> symbolTable;
    public Map<String, WarpScriptStackFunction> defined;
    public Object[] registers;
  }

  public StoreClient getStoreClient() {
    return this.storeClient;
  }

  public DirectoryClient getDirectoryClient() {
    return this.directoryClient;
  }

  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient) {
    this(storeClient, directoryClient, DEFAULT_PROPERTIES);
  }

  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient, Properties properties) {
    this(storeClient, directoryClient, properties, true);
  }

  public MemoryWarpScriptStack(StoreClient storeClient, DirectoryClient directoryClient, Properties properties, boolean init) {
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;

    if (null == properties) {
      throw new RuntimeException("Warp 10 configuration not set.");
    }

    this.unshadow = "true".equals(properties.getProperty(Configuration.WARPSCRIPT_DEF_UNSHADOW));

    if (init) {
      setAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH, 0);
      setAttribute(WarpScriptStack.ATTRIBUTE_JSON_STRICT, false);
      setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT, new AtomicLong(0L));
      setAttribute(WarpScriptStack.ATTRIBUTE_GTS_COUNT, new AtomicLong(0L));
      setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_FETCH, Long.toString(WarpScriptStack.DEFAULT_FETCH_LIMIT))));
      setAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_GTS, Long.toString(WarpScriptStack.DEFAULT_GTS_LIMIT))));
      setAttribute(WarpScriptStack.ATTRIBUTE_ELAPSED, new ArrayList<Long>());
      setAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_LOOP_DURATION, Long.toString(WarpScriptStack.DEFAULT_MAX_LOOP_DURATION))));
      setAttribute(WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH, Integer.parseInt(properties.getProperty(Configuration.WARPSCRIPT_MAX_RECURSION, Integer.toString(WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_OPS, Long.toString(WarpScriptStack.DEFAULT_MAX_OPS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS, Integer.parseInt(properties.getProperty(Configuration.WARPSCRIPT_MAX_SYMBOLS, Integer.toString(WarpScriptStack.DEFAULT_MAX_SYMBOLS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.parseInt(properties.getProperty(Configuration.WARPSCRIPT_MAX_DEPTH, Integer.toString(WarpScriptStack.DEFAULT_MAX_DEPTH))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_BUCKETS, Integer.toString(WarpScriptStack.DEFAULT_MAX_BUCKETS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_PIXELS, Long.toString(WarpScriptStack.DEFAULT_MAX_PIXELS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_GEOCELLS, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_GEOCELLS, Integer.toString(WarpScriptStack.DEFAULT_MAX_GEOCELLS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_JSON_MAXSIZE, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_JSON, Long.toString(WarpScriptStack.DEFAULT_MAX_JSON))));

      //
      // Set hard limits
      //

      setAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_LOOP_DURATION_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_LOOP_DURATION))));
      setAttribute(WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH_HARD, Integer.parseInt(properties.getProperty(Configuration.WARPSCRIPT_MAX_RECURSION_HARD, Integer.toString(WarpScriptStack.DEFAULT_MAX_RECURSION_LEVEL))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH_HARD, Integer.parseInt(properties.getProperty(Configuration.WARPSCRIPT_MAX_DEPTH_HARD, Integer.toString(WarpScriptStack.DEFAULT_MAX_DEPTH))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_OPS_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_OPS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS_HARD, Integer.parseInt(properties.getProperty(Configuration.WARPSCRIPT_MAX_SYMBOLS_HARD, Integer.toString(WarpScriptStack.DEFAULT_MAX_SYMBOLS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_BUCKETS_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_BUCKETS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_PIXELS_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_PIXELS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_FETCH_HARD, Long.toString(WarpScriptStack.DEFAULT_FETCH_LIMIT))));
      setAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_GTS_HARD, Long.toString(WarpScriptStack.DEFAULT_GTS_LIMIT))));
      setAttribute(WarpScriptStack.ATTRIBUTE_MAX_GEOCELLS_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_GEOCELLS_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_GEOCELLS))));
      setAttribute(WarpScriptStack.ATTRIBUTE_JSON_MAXSIZE_HARD, Long.parseLong(properties.getProperty(Configuration.WARPSCRIPT_MAX_JSON_HARD, Long.toString(WarpScriptStack.DEFAULT_MAX_JSON))));

      //
      // Set top level section name
      //

      setAttribute(WarpScriptStack.ATTRIBUTE_SECTION_NAME, WarpScriptStack.TOP_LEVEL_SECTION);

      //
      // Initialize counters
      //

      this.counters = new AtomicLong[1];

      for (int i = 0; i < this.counters.length; i++) {
        this.counters[i] = new AtomicLong(0L);
      }
    }

    this.properties = properties;

    int nregs = Integer.parseInt(this.properties.getProperty(Configuration.CONFIG_WARPSCRIPT_REGISTERS, String.valueOf(WarpScriptStack.DEFAULT_REGISTERS)));
    allowLooseBlockComments = "true".equals(properties.getProperty(Configuration.WARPSCRIPT_ALLOW_LOOSE_BLOCK_COMMENTS, "false"));
    this.registers = new Object[nregs];
  }

  public void maxLimits() {
    setAttribute(WarpScriptStack.ATTRIBUTE_FETCH_LIMIT, Long.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_GTS_LIMIT, Long.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_LOOP_MAXDURATION, Long.MAX_VALUE);
    setAttribute(WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH, Integer.MAX_VALUE);
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS, Integer.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS, Long.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS, Long.MAX_VALUE - 1);
    // Set max of geocells to the largest INTEGER - 1, not Long.MAX_VALUE as it is used as an int
    setAttribute(WarpScriptStack.ATTRIBUTE_MAX_GEOCELLS, Integer.MAX_VALUE - 1);
    setAttribute(WarpScriptStack.ATTRIBUTE_JSON_MAXSIZE, Long.MAX_VALUE);
  }

  @Override
  public int depth() {
    return size;
  }

  @Override
  public void reset(int depth) throws WarpScriptException {
    if (depth < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    if (size > depth) {
      size = depth;
    }
  }

  @Override
  public void clear() {
    size = 0;
  }

  @Override
  public void drop() throws EmptyStackException {
    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    size--;
  }

  @Override
  public void dropn() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();

    if (size < n || n < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    size -= n;
  }

  @Override
  public void dup() throws EmptyStackException, WarpScriptException {
    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    Object element = elements[offset + size - 1];
    ensureCapacity(1);
    elements[offset + size++] = element;
  }

  @Override
  public void dupn() throws EmptyStackException, IndexOutOfBoundsException, WarpScriptException {
    int n = getn();

    if (size < n || n < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    int count = n;
    ensureCapacity(n);
    while (count > 0) {
      Object o = elements[offset + size - 1 - (n - 1)];
      elements[offset + size++] = o;
      count--;
    }
  }

  @Override
  public Object pop() throws EmptyStackException {
    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    Object element = elements[offset + size - 1];
    size--;

    return element;
  }

  @Override
  public Object[] popn() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();

    return popn(n);
  }

  @Override
  public Object[] popn(int n) throws EmptyStackException, IndexOutOfBoundsException {
    if (size < n || n < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    Object[] objects = new Object[n];

    System.arraycopy(elements, offset + size - n, objects, 0, n);

    size -= n;

    return objects;
  }

  @Override
  public void push(Object o) throws WarpScriptException {
    ensureCapacity(1);
    elements[offset + size++] = o;
  }

  @Override
  public void swap() throws WarpScriptException, EmptyStackException, IndexOutOfBoundsException {
    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    if (size < 2) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    Object top = elements[offset + size - 1];
    Object top2 = elements[offset + size - 2];
    elements[offset + size - 1] = top2;
    elements[offset + size - 2] = top;
  }

  @Override
  public Object peek() throws EmptyStackException {
    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    return elements[offset + size - 1];
  }

  @Override
  public void rot() throws EmptyStackException, IndexOutOfBoundsException {

    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    if (size < 3) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    Object tmp = elements[offset + size - 1 - 2];

    for (int i = 0; i < 2; i++) {
      elements[offset + size - 1 - 2 + i] = elements[offset + size - 1 - 2 + i + 1];
    }

    elements[offset + size - 1] = tmp;
  }

  @Override
  public void roll() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();

    if (size < n || n < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    Object tmp = elements[offset + size - 1 - (n - 1)];

    for (int i = 0; i < n - 1; i++) {
      elements[offset + size - 1 - (n - 1) + i] = elements[offset + size - 1 - (n - 1) + i + 1];
    }

    elements[offset + size - 1] = tmp;
  }

  @Override
  public Object peekn() throws WarpScriptException {
    int n = getn();

    return get(n);
  }

  @Override
  public Object get(int n) throws WarpScriptException {
    if (size - 1 < n || n < 0) {
      throw new WarpScriptException("Invalid level.");
    }

    return elements[offset + size - 1 - n];
  }

  /**
   * Turn on/off auditMode. In auditMode, macros contains special statements with line numbers or WarpScript parsing errors.
   * Parsing errors are also stored into ATTRIBUTE_PARSING_ERRORS stack attribute, to avoid walking in macros.
   * auditMode exits after the closing the first last macro level, leaving on stack a macro object.
   */
  @Override
  public void auditMode(boolean auditMode) {
    if (auditMode) {
      setAttribute(ATTRIBUTE_PARSING_ERRORS, new ArrayList<WarpScriptAuditStatement>());
    }
    this.auditMode = auditMode;
  }

  /**
   * Saves the statement to the error list contained in ATTRIBUTE_PARSING_ERRORS stack attribute.
   *
   * @param st list of unknown functions or exceptions
   */
  private void addAuditError(WarpScriptAuditStatement st) {
    Object o = getAttribute(ATTRIBUTE_PARSING_ERRORS);
    if (o instanceof List) {
      ((List) o).add(st);
    }
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
    if (0 == size) {
      throw new InformativeEmptyStackException();
    }

    //
    // Extract the top of the stack and use it as 'N'
    //

    Object o = pop();

    if (! (o instanceof Number)) {
      throw new IndexOutOfBoundsException("Unexpected type, expecting a numerical value.");
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

        exec(line, i);
        i++;
      }
      br.close();
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    } catch (WarpScriptStopException wsse) {
      // Rethrow WarpScriptStopExceptions as is
      throw wsse;
    } catch (Exception e) {
      throw new WarpScriptException("Line #" + i, e);
    }

    //String[] lines = UnsafeString.split(script, '\n');
    //for (String line: lines) {
    //  exec(line);
    //}
  }

  @Override
  public void exec(String line) throws WarpScriptException {
    exec(line, -1);
  }

  public void exec(String line, long lineNumber) throws WarpScriptException {

    String rawline = line;

    try {
      recurseIn();

      String[] statements;

      line = line.trim();

      //
      // Replace whitespaces in Strings with '%20'
      //

      line = UnsafeString.sanitizeStrings(line);

      if (-1 != line.indexOf(' ') && !inMultiline.get()) {
        //statements = line.split(" +");
        statements = UnsafeString.split(line, ' ');
      } else {
        // We're either in multiline mode or the line had no whitespace inside
        statements = new String[1];
        if (inMultiline.get()) {
          // If the line only contained the end of multiline indicator with possible wsp on both sides
          // then set the statement to that, otherwise set it to the raw line
          if(WarpScriptStack.MULTILINE_END.equals(line)) {
            statements[0] = line;
          } else {
            statements[0] = rawline;
          }
        } else {
          statements[0] = line;
        }
      }

      //
      // Report progress
      //

      progress();

      //
      // Loop over the statements
      //

      for (int st = 0; st < statements.length; st++) {
        handleSignal();

        String stmt = statements[st];

        try {
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

          if (stmt.length() > 0 && (stmt.charAt(0) == '#' || (stmt.charAt(0) == '/' && stmt.length() >= 2 && stmt.charAt(1) == '/')) && !inMultiline.get() && !inComment.get()) {
            // Skip comments and blank lines
            return;
          }

          if (WarpScriptStack.MULTILINE_END.equals(stmt)) {
            if (!inMultiline.get()) {
              if (auditMode && !(macros.isEmpty() || macros.size() == forcedMacro)) {
                WarpScriptAuditStatement err = new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_EXCEPTION, null, "Not inside a multiline.", lineNumber, st);
                macros.get(0).add(err);
                addAuditError(err);
              } else {
                throw new WarpScriptException("Not inside a multiline.");
              }
            }
            inMultiline.set(false);

            String mlcontent = multiline.toString();

            if (null != secureScript) {
              secureScript.append(" ");
              secureScript.append("'");
              try {
                secureScript.append(WarpURLEncoder.encode(mlcontent, StandardCharsets.UTF_8));
              } catch (UnsupportedEncodingException uee) {
              }
              secureScript.append("'");
            } else {
              if (macros.isEmpty()) {
                this.push(mlcontent);
              } else {
                macros.get(0).add(mlcontent);
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
          } else if (!allowLooseBlockComments && WarpScriptStack.COMMENT_END.equals(stmt)) {
            // Legacy comments block: Comments block must start with /* and end with */ .
            if (!inComment.get()) {
              if (auditMode && !(macros.isEmpty() || macros.size() == forcedMacro)) {
                WarpScriptAuditStatement err = new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_EXCEPTION, null, "Not inside a comment.", lineNumber, st);
                macros.get(0).add(err);
                addAuditError(err);
              } else {
                throw new WarpScriptException("Not inside a comment.");
              }
            }
            inComment.set(false);
            continue;
          } else if (allowLooseBlockComments && stmt.startsWith(WarpScriptStack.COMMENT_START) && stmt.endsWith(WarpScriptStack.COMMENT_END)) {
            // Single statement case : /*****foo*****/
            continue;
          } else if (allowLooseBlockComments && inComment.get() && stmt.endsWith(WarpScriptStack.COMMENT_END)) {
            // End of comment, statement may contain characters before : +-+***/
            inComment.set(false);
            continue;
          } else if (inComment.get()) {
            continue;
          } else if (!allowLooseBlockComments && WarpScriptStack.COMMENT_START.equals(stmt)) {
            // Start of comment, statement may contain characters after : /**----
            inComment.set(true);
            continue;
          } else if (allowLooseBlockComments && stmt.startsWith(WarpScriptStack.COMMENT_START)) {
            // Legacy comments block: Comments block must start with /* and end with */ .
            inComment.set(true);
            continue;
          } else if (WarpScriptStack.MULTILINE_START.equals(stmt)) {
            if (1 != statements.length) {
              if (auditMode && !(macros.isEmpty() || macros.size() == forcedMacro)) {
                WarpScriptAuditStatement err = new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_EXCEPTION, null, "Can only start multiline strings by using " + WarpScriptStack.MULTILINE_START + " on a line by itself.", lineNumber, st);
                macros.get(0).add(err);
                addAuditError(err);
              } else {
                throw new WarpScriptException("Can only start multiline strings by using " + WarpScriptStack.MULTILINE_START + " on a line by itself.");
              }
            }
            inMultiline.set(true);
            multiline = new StringBuilder();
            continue;
          }

          incOps();
          checkOps();

          if (WarpScriptStack.SECURE_SCRIPT_END.equals(stmt)) {
            if (null == secureScript) {
              if (auditMode && !(macros.isEmpty() || macros.size() == forcedMacro)) {
                WarpScriptAuditStatement err = new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_EXCEPTION, null, "Not inside a secure script definition.", lineNumber, st);
                macros.get(0).add(err);
                addAuditError(err);
              } else {
                throw new WarpScriptException("Not inside a secure script definition.");
              }
            } else {
              this.push(secureScript.toString());
              secureScript = null;
              new SECURE("SECURESCRIPT").apply(this);
            }
          } else if (WarpScriptStack.SECURE_SCRIPT_START.equals(stmt)) {
            if (null == secureScript) {
              secureScript = new StringBuilder();
            } else {
              if (auditMode && !(macros.isEmpty() || macros.size() == forcedMacro)) {
                WarpScriptAuditStatement err = new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_EXCEPTION, null, "Already inside a secure script definition.", lineNumber, st);
                macros.get(0).add(err);
                addAuditError(err);
              } else {
                throw new WarpScriptException("Already inside a secure script definition.");
              }
            }
          } else if (null != secureScript) {
            secureScript.append(" ");
            secureScript.append(stmt);
          } else if (WarpScriptStack.MACRO_END.equals(stmt)) {
            if (macros.isEmpty() || macros.size() == forcedMacro) {
              throw new WarpScriptException("Not inside a macro definition.");
            } else {
              Macro lastmacro = macros.remove(0);
              if (auditMode && (macros.isEmpty() || macros.size() == forcedMacro)) {
                auditMode = false;  // the only way to exit audit mode is to close the last level of opened macro.
              }
              boolean secure = Boolean.TRUE.equals(this.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO));

              if (!Boolean.TRUE.equals(this.getAttribute(WarpScriptStack.ATTRIBUTE_IN_XEVAL))) {
                lastmacro.setSecure(secure);
              }

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
          } else if (stmt.length() > 1 && ((stmt.charAt(0) == '\'' && stmt.charAt(stmt.length() - 1) == '\'')
              || (stmt.charAt(0) == '\"' && stmt.charAt(stmt.length() - 1) == '\"'))) {
            //
            // Push Strings onto the stack
            //

            try {
              String str = stmt.substring(1, stmt.length() - 1);

              str = WarpURLDecoder.decode(str, StandardCharsets.UTF_8);

              if (macros.isEmpty()) {
                push(str);
              } else {
                macros.get(0).add(str);
              }
            } catch (UnsupportedEncodingException uee) {
              // Cannot happen...
              throw new WarpScriptException(uee);
            }
          } else if (stmt.length() > 2 && stmt.charAt(1) == 'x' && stmt.charAt(0) == '0') {
            long hexl = stmt.length() < 18 ? Long.parseLong(stmt.substring(2), 16) : new BigInteger(stmt.substring(2), 16).longValue();
            if (macros.isEmpty()) {
              push(hexl);
            } else {
              macros.get(0).add(hexl);
            }
          } else if (stmt.length() > 2 && stmt.charAt(1) == 'b' && stmt.charAt(0) == '0') {
            long binl = stmt.length() < 66 ? Long.parseLong(stmt.substring(2), 2) : new BigInteger(stmt.substring(2), 2).longValue();
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
                if (!getSymbolTable().containsKey(stmt.substring(1))) {
                  throw new WarpScriptException("Unknown symbol '" + stmt.substring(1) + "'");
                }
              }

              push(o);
            } else {
              if (auditMode) {
                macros.get(0).add(new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_LOAD,
                    stmt.substring(1), stmt, lineNumber, st));
              } else {
                macros.get(0).add(stmt.substring(1));
                macros.get(0).add(WarpScriptLib.getFunction(WarpScriptLib.LOAD));
              }
            }
          } else if (stmt.startsWith("!$")) {
            //
            // This is an immediate variable dereference
            //
            if (auditMode && macros.size() > 1) {
              macros.get(0).add(new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_EARLY_BINDING, null, stmt.substring(2), lineNumber, st));
            } else {
              Object o = load(stmt.substring(2));

              if (null == o) {
                if (!getSymbolTable().containsKey(stmt.substring(2))) {
                  if (0 == forcedMacro) {
                    if (macros.size() > 1) {
                      throw new WarpScriptException("Early binding is not possible inside a macro.");
                    } else {
                      throw new WarpScriptException("Unknown symbol '" + stmt.substring(2) + "'");
                    }
                  } else {
                    throw new WarpScriptException("Early binding is not compatible with time execution limits such as " + Configuration.EGRESS_MAXTIME +
                        " configuration or " + WarpScriptStack.CAPABILITY_TIMEBOX_MAXTIME + " capability.");
                  }
                }
              }

              if (macros.isEmpty()) {
                push(o);
              } else {
                macros.get(0).add(o);
              }
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
              if (auditMode) {
                macros.get(0).add(new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.WS_LOAD,
                    stmt.substring(1), stmt, lineNumber, st));
              } else {
                macros.get(0).add(WarpScriptLib.getFunction(WarpScriptLib.RUN));
              }
            }
          } else {
            //
            // This is a function call
            //
            if (auditMode && !(macros.isEmpty() || macros.size() == forcedMacro)) {
              try {
                Object func = findFunction(stmt);
                macros.get(0).add(new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.FUNCTION_CALL, func, stmt, lineNumber, st));
              } catch (WarpScriptException e) {
                WarpScriptAuditStatement err = new WarpScriptAuditStatement(WarpScriptAuditStatement.STATEMENT_TYPE.UNKNOWN, null, stmt, lineNumber, st);
                macros.get(0).add(err);
                addAuditError(err);
              }
            } else {
              Object func = findFunction(stmt);

              //
              // Check WarpScript functions
              //

              Map<String, String> labels = new HashMap<String, String>();
              labels.put(SensisionConstants.SENSISION_LABEL_FUNCTION, stmt);

              long nano = System.nanoTime();

              try {
                if (func instanceof WarpScriptStackFunction && macros.isEmpty()) {
                  //
                  // Function is an WarpScriptStackFunction, call it on this stack
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
                if (functionMetrics) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FUNCTION_COUNT, labels, 1);
                  Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FUNCTION_TIME_US, labels, (System.nanoTime() - nano) / 1000L);
                }
              }
            }
          }
        } catch (WarpScriptATCException e) {
          throw e;
        } catch (Exception e) {
          StringBuilder errorMessage = new StringBuilder("Exception at '");
          boolean nextStatement = false;
          for (int stc = Math.max(0, st - 3); stc < Math.min(statements.length, st + 4); stc++) {
            if (nextStatement) {
              errorMessage.append(" ");
            } else {
              nextStatement = true;
            }

            if (st == stc) {
              errorMessage.append("=>").append(StringUtils.abbreviateMiddle(statements[stc].trim(), "...", 32)).append("<=");
            } else {
              errorMessage.append(StringUtils.abbreviateMiddle(statements[stc], "...", 32));
            }
          }
          errorMessage.append("' in section " + sectionName);

          throw new WarpScriptException(errorMessage.toString(), e);
        }
      }

      return;
    } finally {
      recurseOut();
    }
  }

  @Override
  public void exec(Macro macro) throws WarpScriptException {

    // We increment op count for the macro itself. We'll increment
    // for each statement of the macro inside the loop
    incOps();

    boolean secure = this.inSecureMacro; //Boolean.TRUE.equals(this.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO));

    //
    // Save current section name
    //

    String sectionname = this.sectionName; //(String) this.getAttribute(WarpScriptStack.ATTRIBUTE_SECTION_NAME);

    //
    // If we are already in a secure macro, stay in this mode, otherwise an inner macro could lower the
    // secure level
    //

    //this.setAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO, !secure ? macro.isSecure() : secure);
    this.inSecureMacro = this.inSecureMacro || macro.isSecure();

    int i = 0;

    int n = macro.size();

    String macroname = this.macroName;
    this.macroName = macro.getName();

    try {

      recurseIn();

      // Notify progress
      progress();

      for (i = 0; i < n; i++) {
        handleSignal();

        Object stmt = macro.get(i);

        incOps();

        if (stmt instanceof WarpScriptStackFunction) {
          WarpScriptStackFunction esf = (WarpScriptStackFunction) stmt;

          // FIXME(hbs): we do not count the number of calls or the time spent in individual functions
          // called from macros - We could correct that but it would have a great impact on performance
          // as we would be calling sensision update potentially several billion times per script

          //long nano = System.nanoTime();

          esf.apply(this);

          //Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FUNCTION_COUNT, esf.getSensisionLabels(), 1);
          //Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_FUNCTION_TIME_US, esf.getSensisionLabels(), (System.nanoTime() - nano) / 1000L);
        } else {
          push(stmt);
        }
      }

      checkOps();
    } catch (WarpScriptReturnException ere) {
      if (this.getCounter(WarpScriptStack.COUNTER_RETURN_DEPTH).decrementAndGet() > 0) {
        throw ere;
      }
    } catch (WarpScriptATCException wsatce) {
      throw wsatce;
    } catch (Exception ee) {
      if (this.inSecureMacro) {
        throw ee;
      } else {
        String name = macro.getName();
        String section = (String) this.getAttribute(WarpScriptStack.ATTRIBUTE_SECTION_NAME);
        Object statement = macro.get(i);
        if (i >= macro.size()) {
          statement = macro.get(macro.size() - 1);
        }
        String statementString = String.valueOf(statement);
        // For NamedWarpScriptFunction, toString is used for snapshotting. Getting the name is better to generate
        // a clear error message.
        if(statement instanceof NamedWarpScriptFunction) {
          String funcName = ((NamedWarpScriptFunction) statement).getName();
          if(null != funcName) {
            statementString = funcName;
          }
        }
        if (null == name) {
          throw new WarpScriptException("Exception" + (i < n ? (" at '" + statementString + "'") : "") + " in section '" + section + "'", ee);
        } else {
          throw new WarpScriptException("Exception" + (i < n ? (" at '" + statementString + "'") : "") + " in section '" + section + "' called from macro '" + name + "'", ee);
        }
      }
    } finally {
      //this.setAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO, secure);
      this.inSecureMacro = secure;
      recurseOut();
      // Restore section name
      this.sectionName = sectionname;
      // Restore macro name
      this.macroName = macroname;

      //if (sectionname != this.getAttribute(WarpScriptStack.ATTRIBUTE_SECTION_NAME)) {
      //  this.setAttribute(WarpScriptStack.ATTRIBUTE_SECTION_NAME, sectionname);
      //}
    }
  }

  public Object findFunction(String stmt) throws WarpScriptException {
    Object func = defined.get(stmt);

    if (null != func && Boolean.FALSE.equals(getAttribute(WarpScriptStack.ATTRIBUTE_ALLOW_REDEFINED))) {
      throw new WarpScriptException("Disallowed redefined function '" + stmt + "'.");
    }

    func = null != func ? func : WarpScriptLib.getFunction(stmt);

    if (null == func) {
      throw new WarpScriptException("Unknown function '" + stmt + "'");
    }

    return func;
  }

  @Override
  public void run(String symbol) throws WarpScriptException {

    Macro macro = find(symbol);

    //
    // Execute macro
    //

    exec((Macro) macro);
  }

  @Override
  public Macro find(String symbol) throws WarpScriptException {

    //
    // Check if we have import rules which must be applied
    //

    symbol = rewriteMacroSymbol(symbol);

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
      macro = WarpFleetMacroRepository.find(this, symbol);
    }

    if (null == macro) {
      macro = MacroResolver.find(this, symbol);
    }

    if (null == macro) {
      throw new WarpScriptException("Unknown macro '" + symbol + "'");
    }

    if (!(macro instanceof Macro)) {
      throw new WarpScriptException("'" + symbol + "' is not a macro.");
    }

    return (Macro) macro;
  }

  @Override
  public String dump(int n) {
    StringBuilder sb = new StringBuilder();

    if (n > this.size) {
      n = this.size;
    }

    for (int i = n - 1; i >= 0; i--) {
      if (i < this.size) {
        sb.append(i + 1);
        sb.append(": ");
        Object elt = this.elements[offset + this.size - 1 - i];

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
  public void pick() throws EmptyStackException, IndexOutOfBoundsException, WarpScriptException {
    int n = getn();

    if (size < n || n < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    ensureCapacity(1);
    Object o = elements[offset + size - 1 - (n - 1)];
    elements[offset + size++] = o;
  }

  @Override
  public void rolld() throws EmptyStackException, IndexOutOfBoundsException {
    int n = getn();

    if (size < n || n < 0) {
      throw new IndexOutOfBoundsException("Index out of bound.");
    }

    Object tmp = elements[offset + size - 1];
    for (int i = 0; i < n - 1; i++) {
      elements[offset + size - 1 - i] = elements[offset + size - 1 - (i + 1)];
    }
    elements[offset + size - 1 - (n - 1)] = tmp;
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
  public Object load(int regidx) throws WarpScriptException {
    if (regidx >= 0 && regidx < registers.length) {
      return this.registers[regidx];
    }

    throw new WarpScriptException("Invalid register number, must be between 0 and " + (registers.length - 1));
  }

  @Override
  public void store(int regidx, Object value) throws WarpScriptException {
    if (regidx < 0 || regidx >= registers.length) {
      throw new WarpScriptException("Invalid register number, must be between 0 and " + (registers.length - 1));
    }

    this.registers[regidx] = value;
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
  public Object[] getRegisters() {
    return this.registers;
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

    if (null == value) {
      return this.attributes.remove(key);
    }

    //
    // Handle in_secure_macro separately as we set it very often
    // in loops
    //

    if (WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO.equals(key)) {
      boolean old = this.inSecureMacro;
      this.inSecureMacro = Boolean.TRUE.equals(value);
      return old;
    }

    if (WarpScriptStack.ATTRIBUTE_MAX_DEPTH.equals(key)) {
      this.maxdepth = ((Number) value).intValue();
      // Check if the underlying array is already bigger than the requested maximum depth
      if (elements.length > this.maxdepth) {
        if (size + offset > maxdepth) {
          throw new IndexOutOfBoundsException("The stack depth is over the requested maximum depth.");
        } else {
          elements = Arrays.copyOf(elements, maxdepth);
        }
      }
    } else if (WarpScriptStack.ATTRIBUTE_MAX_OPS.equals(key)) {
      this.maxops = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_RECURSION_MAXDEPTH.equals(key)) {
      this.maxrecurse = ((Number) value).intValue();
    } else if (WarpScriptStack.ATTRIBUTE_MAX_SYMBOLS.equals(key)) {
      this.maxsymbols = ((Number) value).intValue();
    } else if (WarpScriptStack.ATTRIBUTE_OPS.equals(key)) {
      this.currentops = ((Number) value).longValue();
    } else if (WarpScriptStack.ATTRIBUTE_SECTION_NAME.equals(key)) {
      this.sectionName = value.toString();
    } else if (WarpScriptStack.ATTRIBUTE_MACRO_NAME.equals(key)) {
      this.macroName = value.toString();
    } else if (WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE.equals(key)) {
      // value is not null because it was checked on first line
      this.progressable = (Progressable) value;
    } else if (WarpScriptStack.ATTRIBUTE_NAME.equals(key)) {
      // Register the stack if its name is set, this will avoid
      // having lots of anonymous stacks being registered
      WarpScriptStackRegistry.register(this);
    }

    return this.attributes.put(key, value);
  }

  @Override
  public Object getAttribute(String key) {
    // Manage the number of ops in a special way
    if (WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO.equals(key)) {
      return this.inSecureMacro;
    } else if (WarpScriptStack.ATTRIBUTE_OPS.equals(key)) {
      return this.currentops;
    } else if (WarpScriptStack.ATTRIBUTE_SECTION_NAME.equals(key)) {
      return this.sectionName;
    } else if (WarpScriptStack.ATTRIBUTE_MACRO_NAME.equals(key)) {
      return this.macroName;
    } else if (WarpScriptStack.ATTRIBUTE_CREATION_TIME.equals(key)) {
      return this.creationTime;
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
  public void incOps() throws WarpScriptException {
    this.currentops++;
  }

  public void checkOps() throws WarpScriptException {
    if (this.currentops > this.maxops) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_OPSCOUNT_EXCEEDED, Sensision.EMPTY_LABELS, 1);
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
  public void define(final String stmt, final Macro macro) {
    if (null == macro) {
      if (this.unshadow) {
        this.defined.remove(stmt);
      } else {
        Macro undefMacro = new Macro();
        undefMacro.add("is undefined.");
        undefMacro.add(new MSGFAIL(stmt));
        this.defined.put(stmt, MacroHelper.wrap(stmt, undefMacro));
      }
    } else {
      this.defined.put(stmt, MacroHelper.wrap(stmt, macro));
    }
  }

  @Override
  public void save() throws WarpScriptException {
    //
    // Create a new StackContext
    //

    StackContext context = new StackContext();

    //
    // Copy symbol table and registers
    //

    context.symbolTable = new HashMap<String, Object>(this.symbolTable.size());
    context.symbolTable.putAll(this.symbolTable);
    context.registers = Arrays.copyOf(this.registers, this.registers.length);

    //
    // Copy redefined functions
    //

    context.defined = new HashMap<String, WarpScriptStackFunction>(this.defined.size());
    context.defined.putAll(this.defined);

    //
    // Push context onto the stack
    //

    this.push(context);
  }

  @Override
  public void restore(io.warp10.script.WarpScriptStack.StackContext ctxt) throws WarpScriptException {
    if (!(ctxt instanceof StackContext)) {
      throw new WarpScriptException("Invalid context type.");
    }

    StackContext context = (StackContext) ctxt;

    //
    // Restore symbol table and registers
    //

    this.symbolTable.clear();

    if (null != context.symbolTable) {
      this.symbolTable.putAll(context.symbolTable);
    }

    System.arraycopy(context.registers, 0, this.registers, 0, this.registers.length);

    //
    // Restore redefined functions
    //

    this.defined.clear();
    if (null != context.defined) {
      this.defined.putAll(context.defined);
    }
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

    this.restore(context);
  }

  private long reclevel = 0;
  protected void recurseIn() throws WarpScriptException {
    if (++this.reclevel > this.maxrecurse) {
      throw new WarpScriptException("Maximum recursion level reached (" + this.reclevel + ")");
    }
  }

  //protected void recurseIn() throws WarpScriptException {
  //  if (this.recursionLevel.addAndGet(1) > this.maxrecurse) {
  //    throw new WarpScriptException("Maximum recursion level reached (" + this.recursionLevel.get() + ")");
  //  }
  //}

  //protected void recurseOut() {
  //  this.recursionLevel.addAndGet(-1);
  //}

  protected void recurseOut() {
    this.reclevel--;
  }

  // Current call graph depth
  public long getRecursionLevel() {
    return this.reclevel;
  }

  // Depth of macros being currently defined
  public int getMacroDepth() {
    return this.macros.size();
  }

  public boolean isInMultiline() {
    return this.inMultiline.get();
  }

  public boolean isInComment() {
    return this.inComment.get();
  }

  public boolean isInSecureScript() {
    return null != this.secureScript;
  }

  /**
   * Create a 'sub' stack of the current one.
   * A substack will share a certain number of elements with its parent stack.
   */
  public MemoryWarpScriptStack getSubStack() {

    final MemoryWarpScriptStack parentStack = this;

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(getStoreClient(), getDirectoryClient(), properties, false) {

      private final Map<String,Object> attributes = new HashMap<String, Object>();

      @Override
      public void incOps() throws WarpScriptException {
        parentStack.incOps();
      }

      @Override
      public void checkOps() throws WarpScriptException {
        parentStack.checkOps();
      }

      @Override
      public Object getAttribute(String key) {
        //
        // The secure mode is to be treated differently as we don't want to allow
        // privilege escalation
        //
        if (WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO.equals(key)) {
          if (Boolean.TRUE.equals(parentStack.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO))) {
            return true;
          } else {
            return this.attributes.get(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO);
          }
        } else {
          return parentStack.getAttribute(key);
        }
      }

      @Override
      public Object setAttribute(String key, Object value) {
        //
        // The secure mode is to be treated differently as we don't want to allow
        // privilege escalation
        //
        if (WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO.equals(key)) {
          if (!Boolean.TRUE.equals(parentStack.getAttribute(WarpScriptStack.ATTRIBUTE_IN_SECURE_MACRO))) {
            return this.attributes.put(key, value);
          } else {
            return parentStack.getAttribute(key);
          }
        } else {
          return parentStack.setAttribute(key, value);
        }
      }

      @Override
      protected void recurseIn() throws WarpScriptException {
        parentStack.recurseIn();
      }

      @Override
      protected void recurseOut() {
        parentStack.recurseOut();
      }
    };

    //
    // Set some levels
    //

    stack.maxdepth = this.maxdepth;
    stack.counters = this.counters;
    stack.maxops = this.maxops;
    stack.maxrecurse = this.maxrecurse;
    stack.maxsymbols = this.maxsymbols;
    return stack;
  }

  private void ensureCapacity(int n) throws WarpScriptException {
    if (offset + size + n < elements.length) {
      return;
    }

    if (offset + size + n > this.maxdepth) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_STACKDEPTH_EXCEEDED, Sensision.EMPTY_LABELS, 1);
      throw new WarpScriptException("Stack depth would exceed set limit of " + this.maxdepth);
    }

    int newCapacity = Math.min(this.maxdepth, elements.length + (elements.length >> 1) + n);
    elements = Arrays.copyOf(elements, newCapacity);
  }

  private String rewriteMacroSymbol(String symbol) {
    Map<String,String> rules = (Map<String,String>) this.attributes.get(WarpScriptStack.ATTRIBUTE_IMPORT_RULES);

    if (null == rules) {
      return symbol;
    }

    //
    // Scan the rules, from longest to shortest. We can do that because the underneath implementation
    // is a TreeMap and TreeMap.entrySet returns the entries in ascending key order.
    //

    for (Map.Entry<String, String> prefixAndSubstitute: rules.entrySet()) {
      String prefix = prefixAndSubstitute.getKey();
      String substitute = prefixAndSubstitute.getValue();

      if (symbol.startsWith(prefix)) {
        symbol = substitute + symbol.substring(prefix.length());
        break;
      }
    }

    return symbol;
  }

  @Override
  public void signal(Signal signal) {
    //
    // Only set the signal is 'signal' is of higher priority than the current
    // signal
    //

    // Only set the signal if the stack is not yet signaled or if 'signal' is
    // of higher priority than the current signal

    synchronized(this) {
      if (!this.signaled || this.signal.ordinal() < signal.ordinal()) {
        this.signal = signal;
        this.signaled = true;
      }
    }
  }

  @Override
  public void handleSignal() throws WarpScriptATCException {
    if (this.signaled) {
      doSignal();
    }
  }

  private void doSignal() throws WarpScriptATCException {
    synchronized(this) {
      switch (this.signal) {
        case STOP:
          // Clear the signal
          this.signal = null;
          this.signaled = false;
          throw new WarpScriptStopException("Execution received STOP signal.");
        case KILL:
          // The signal is retained
          throw new WarpScriptKillException("Execution received KILL signal.");
        default:
      }
    }
  }

  public void setFunctionMetrics(boolean state) {
    this.functionMetrics = state;
  }

  @Override
  public int hide() {
    int count = size;
    offset += count;
    size -= count;
    return count;
  }

  @Override
  public int hide(int count) {
    if (0 == count) {
      return 0;
    } else if (count > size) {
      count = size;
    } else if (count < 0) {
      // If count is negative, it represents the number of levels
      // that we want to keep visible, so we add the current size to determine
      // the number of levels to hide. If size is less than -count, less than
      // the requested number of levels will be visible.
      count += size;

      if (count < 0) {
        count = 0;
      }
    }
    offset += count;
    size -= count;
    return count;
  }

  @Override
  public void show() {
    int count = offset;
    offset -= count;
    size += count;
  }

  @Override
  public void show(int count) {
    if (0 == count) {
      return;
    } else if (count > offset) {
      count = offset;
    } else if (count < 0) {
      // Count is negative, so it represents the number of levels we want visible after
      // the call to show. If this number is already reached, count is set to 0.
      // No levels will be hidden by this call.
      count += size;

      if (count > 0) { // There are already more than -'count' levels visible, do not show any more
        count = 0;
      } else if (-count > offset) { // We would need to show more levels than those hidden, so cap count to offset
        count = offset;
      } else { // We will need to show -'count' levels, so negate 'count'
        count = -count;
      }
    }
    offset -= count;
    size += count;
  }

  @Override
  public void macroOpen() throws WarpScriptException {
    // We are already in a forced macro
    if (0 != forcedMacro) {
      throw new WarpScriptException("Already in a forced Macro.");
    } else {
      macros.add(0, new Macro());
      forcedMacro = macros.size();
    }
  }

  @Override
  public void macroClose() throws WarpScriptException {
    // If we are not in forced macro mode, do nothing
    if (0 == forcedMacro) {
      return;
    }
    if (inMultiline.get()) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.MULTILINE_START + " construct.");
    }
    if (inComment.get()) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.COMMENT_START + " construct.");
    }
    if (null != secureScript) {
      throw new WarpScriptException("Unbalanced " + WarpScriptStack.SECURE_SCRIPT_START + " construct.");
    }

    if (macros.size() != forcedMacro) {
      throw new WarpScriptException("Invalid level for closing forced Macro, check that all macros are correctly closed.");
    }
    this.push(macros.remove(0));
    forcedMacro = 0;
  }
}
