//
//   Copyright 2023  SenX S.A.S.
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
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.UnsafeString;
import io.warp10.json.JsonUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class WarpScriptAudit {

  public static final String AUDIT_TASK_3_X_MIGRATION = "audit3x";
  public static final String AUDIT_TASK_PARSING = "parsing";
  private static final String WARPAUDIT_FORMAT = "warpaudit.format";

  private final boolean allowLooseBlockComments = !"false".equals(System.getProperty(Configuration.WARPSCRIPT_ALLOW_LOOSE_BLOCK_COMMENTS));


  public enum ISSUE_GRAVITY {
    ERROR,
    WARNING,
    INFO
  }

  /**
   * Stores an issue, and a possible advice or solution + the issue position
   * Issue gravity is defined by {@link ISSUE_GRAVITY}
   */
  public static class Issue {
    public String issue;
    public Long lineNumber;
    public int statementNumber;
    public String advice = "";
    public ISSUE_GRAVITY gravity;

    public Issue(ISSUE_GRAVITY gravity, String issue, Long lineNumber, int statementNumber) {
      this.issue = issue;
      this.lineNumber = lineNumber;
      this.statementNumber = statementNumber;
      this.gravity = gravity;
    }

    public Issue(ISSUE_GRAVITY gravity, String issue, Long lineNumber, int statementNumber, String advice) {
      this(gravity, issue, lineNumber, statementNumber);
      this.advice = advice;
    }

    @Override
    public String toString() {
      return "AuditResult{" +
          "issue='" + issue + '\'' +
          ", lineNumber=" + lineNumber +
          ", statementNumber=" + statementNumber +
          "}\n";
    }

  }


  /**
   * During the parsing, the parser will store statements with the following types
   */
  public enum STATEMENT_TYPE {
    FUNCTION_CALL,  // function present in WarpScriptLib
    VARIABLE_LOAD,  // either $x or !$x
    STRING,         // 'x' or "x" or multiline. In case of multiline, statement position is the position of the multiline end.
    MACRO_START,    // <%
    MACRO_END,      // %>
    MACRO_CALL,     // @something
    NUMBER,         // long, double, hexadecimal, binary
    BOOLEAN,        // T, F, true, false
    UNKNOWN         // syntax error (missing space, [5 is not a function), function call to a REDEF function, or unknown extension.
  }


  /**
   * Stores a statement, a type, and its position
   * Type is defined by {@link STATEMENT_TYPE}
   */
  public static class Statement {
    public STATEMENT_TYPE type;
    public String statement;
    public Long lineNumber;      // first line is numbered 1
    public int statementNumber; // first statement in line is numbered 0

    public Statement(STATEMENT_TYPE type, String statement, Long lineNumber, int statementNumber) {
      this.type = type;
      this.statement = statement;
      this.lineNumber = lineNumber;
      this.statementNumber = statementNumber;
    }

    @Override
    public String toString() {
      return "Statement{" +
          "type=" + type +
          ", statement='" + statement + '\'' +
          ", lineNumber=" + lineNumber +
          ", statementNumber=" + statementNumber +
          "}\n";
    }
  }

  /**
   * Parse a WarpScript the same way MemoryWarpScriptStack does.
   * Returns a List of Statements or ParsingIssue, if any.
   * A function not in WarpScript lib (or loaded extensions) results in a statement typed UNKNOWN.
   * <p>
   * As parseWarpScriptStatements do not execute any WarpScript, it cannot
   * detect errors produced by external macros or REDEF, or 'wrong ws' EVAL.
   * <p>
   * Syntax, variable name, blocks of code, are similar to MemoryWarpScriptStack.exec()
   *
   * @param ws
   * @return
   */
  public List parseWarpScriptStatements(String ws) {
    ArrayList stList = new ArrayList();
    BufferedReader br = new BufferedReader(new StringReader(ws));

    long lineNumber = 1;

    AtomicBoolean inMultiline = new AtomicBoolean(false);
    AtomicBoolean inComment = new AtomicBoolean(false);
    StringBuilder multiline = null;
    StringBuilder secureScript = null;
    int macroDepth = 0;

    try {
      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }

        // the following is nearly the same as MemoryWarpScriptStack.exec()

        String rawline = line;
        String[] statements;

        line = line.trim();

        line = UnsafeString.sanitizeStrings(line);

        if (-1 != UnsafeString.indexOf(line, ' ') && !inMultiline.get()) {
          //statements = line.split(" +");
          statements = UnsafeString.split(line, ' ');
        } else {
          // We're either in multiline mode or the line had no whitespace inside
          statements = new String[1];
          if (inMultiline.get()) {
            // If the line only contained the end of multiline indicator with possible wsp on both sides
            // then set the statement to that, otherwise set it to the raw line
            if (WarpScriptStack.MULTILINE_END.equals(line)) {
              statements[0] = line;
            } else {
              statements[0] = rawline;
            }
          } else {
            statements[0] = line;
          }
        }

        for (int st = 0; st < statements.length; st++) {

          String stmt = statements[st];

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
            break;
          }

          if (WarpScriptStack.MULTILINE_END.equals(stmt)) {
            if (!inMultiline.get()) {
              stList.add(new Issue(ISSUE_GRAVITY.ERROR, "not inside a multiline string", lineNumber, st));
            } else {
              inMultiline.set(false);
              if (null == multiline) {
                multiline = new StringBuilder();
              }
              String mlcontent = multiline.toString();

              stList.add(new Statement(STATEMENT_TYPE.STRING, mlcontent, lineNumber, st));

              multiline.setLength(0);
              continue;
            }
          } else if (inMultiline.get()) {
            if (multiline.length() > 0) {
              multiline.append("\n");
            }
            multiline.append(stmt);
            continue;
          } else if (!allowLooseBlockComments && WarpScriptStack.COMMENT_END.equals(stmt)) {
            // Legacy comments block: Comments block must start with /* and end with */ .
            if (!inComment.get()) {
              stList.add(new Issue(ISSUE_GRAVITY.ERROR, "strict comments mode, cannot have */ in a statement", lineNumber, st));
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
              stList.add(new Issue(ISSUE_GRAVITY.ERROR, "Can only start multiline strings by using " + WarpScriptStack.MULTILINE_START + " on a line by itself.", lineNumber, st));
            }
            inMultiline.set(true);
            multiline = new StringBuilder();
            continue;
          }

          if (WarpScriptStack.SECURE_SCRIPT_END.equals(stmt)) {
            if (null == secureScript) {
              stList.add(new Issue(ISSUE_GRAVITY.ERROR, "Not inside a secure script definition.", lineNumber, st));
            } else {
              secureScript = null;
            }
          } else if (WarpScriptStack.SECURE_SCRIPT_START.equals(stmt)) {
            if (null == secureScript) {
              secureScript = new StringBuilder(); // we won't build it, it is just for an easy side by side with MemoryWarpScriptStack
            } else {
              stList.add(new Issue(ISSUE_GRAVITY.ERROR, "Not inside a secure script definition.", lineNumber, st));
            }
          } else if (WarpScriptStack.MACRO_END.equals(stmt)) {
            if (macroDepth == 0) {
              stList.add(new Issue(ISSUE_GRAVITY.ERROR, "Not inside a macro definition.", lineNumber, st));
            } else {

              macroDepth--;
              stList.add(new Statement(STATEMENT_TYPE.MACRO_END, stmt, lineNumber, st));
            }
          } else if (WarpScriptStack.MACRO_START.equals(stmt)) {
            //
            // Create holder for current macro
            //
            macroDepth++;
            stList.add(new Statement(STATEMENT_TYPE.MACRO_START, stmt, lineNumber, st));
          } else if ((stmt.charAt(0) == '\'' && stmt.charAt(stmt.length() - 1) == '\'')
              || (stmt.charAt(0) == '\"' && stmt.charAt(stmt.length() - 1) == '\"')) {
            //
            // Push Strings onto the stack
            //

            try {
              String str = stmt.substring(1, stmt.length() - 1);

              str = WarpURLDecoder.decode(str, StandardCharsets.UTF_8);

              stList.add(new Statement(STATEMENT_TYPE.STRING, str, lineNumber, st));

            } catch (UnsupportedEncodingException uee) {
              // Cannot happen...
              throw new RuntimeException(uee);
            }
          } else if (stmt.length() > 2 && stmt.charAt(1) == 'x' && stmt.charAt(0) == '0') {
            long hexl = stmt.length() < 18 ? Long.parseLong(stmt.substring(2), 16) : new BigInteger(stmt.substring(2), 16).longValue();
            stList.add(new Statement(STATEMENT_TYPE.NUMBER, stmt, lineNumber, st));
          } else if (stmt.length() > 2 && stmt.charAt(1) == 'b' && stmt.charAt(0) == '0') {
            long binl = stmt.length() < 66 ? Long.parseLong(stmt.substring(2), 2) : new BigInteger(stmt.substring(2), 2).longValue();
            stList.add(new Statement(STATEMENT_TYPE.NUMBER, stmt, lineNumber, st));
          } else if (UnsafeString.isLong(stmt)) {
            //
            // Push longs onto the stack
            //
            stList.add(new Statement(STATEMENT_TYPE.NUMBER, stmt, lineNumber, st));

          } else if (UnsafeString.isDouble(stmt)) {
            //
            // Push doubles onto the stack
            //
            stList.add(new Statement(STATEMENT_TYPE.NUMBER, stmt, lineNumber, st));

          } else if (stmt.equalsIgnoreCase("T")
              || stmt.equalsIgnoreCase("F")
              || stmt.equalsIgnoreCase("true")
              || stmt.equalsIgnoreCase("false")) {
            //
            // Push booleans onto the stack
            //
            if (stmt.startsWith("T") || stmt.startsWith("t")) {
              stList.add(new Statement(STATEMENT_TYPE.BOOLEAN, stmt, lineNumber, st));
            } else {
              stList.add(new Statement(STATEMENT_TYPE.BOOLEAN, stmt, lineNumber, st));
            }
          } else if (stmt.startsWith("$")) {
            stList.add(new Statement(STATEMENT_TYPE.VARIABLE_LOAD, stmt, lineNumber, st));

          } else if (stmt.startsWith("!$")) {
            //
            // This is an immediate variable dereference
            //
            stList.add(new Statement(STATEMENT_TYPE.VARIABLE_LOAD, stmt, lineNumber, st));

          } else if (stmt.startsWith("@")) {
            stList.add(new Statement(STATEMENT_TYPE.MACRO_CALL, stmt, lineNumber, st));
          } else {
            //
            // This is a function call
            //
            WarpScriptLib wslib = new WarpScriptLib();
            Object func = wslib.getFunction(stmt);
            if (null != func) {
              stList.add(new Statement(STATEMENT_TYPE.FUNCTION_CALL, stmt, lineNumber, st));
            } else {
              stList.add(new Statement(STATEMENT_TYPE.UNKNOWN, stmt, lineNumber, st));
            }
          }
        }
        lineNumber++;
      }
      br.close();
    } catch (IOException e) {
      new RuntimeException("cannot read lines");
    }
    return stList;
  }

  /**
   * Extract the list of issues linked to WarpScript parsing: unknown functions + all the issues raised by 
   * @param ws
   * @return
   */
  public static List<Issue> parsingAudit(String ws) {

    WarpScriptAudit wsa = new WarpScriptAudit();
    List sts = wsa.parseWarpScriptStatements(ws);
    List<Issue> wsParsingIssues = new ArrayList<>();
    // Only keep parsing issues + unknown functions (as warning)
    // TODO(ppn): integrate a mechanism for deprecated function in the future.
    for (Object st: sts) {
      if (st instanceof Issue) {
        wsParsingIssues.add((Issue) st);
      } else if (st instanceof Statement) {
        Statement s = (Statement) st;
        if (s.type == STATEMENT_TYPE.UNKNOWN) {
          wsParsingIssues.add(new Issue(ISSUE_GRAVITY.WARNING, "Unknown function " + s.statement, s.lineNumber, s.statementNumber));
        }
      }
    }
    return wsParsingIssues;
  }

  /**
   * Returns a list of Issue, with advice when possible, to help users to migrate from 2.x to 3.x
   *
   * @param
   * @return
   */
  public static List<Issue> migration3xAudit(String ws) {
    List<Issue> issues = new ArrayList();

    // 3.x removed function.
    HashMap<String, String> removedFunctions = new HashMap<>();
    removedFunctions.put("AUTHENTICATE", "All the functions that needed authentication now needs capabilities"); // TODO(ppn): Add link to capabilities page
    removedFunctions.put("CHUNKENCODER", "`CHUNKENCODER` was replaced by [`CHUNK`](https://www.warp10.io/doc/CHUNK)");
    removedFunctions.put("CUDF", "");
    removedFunctions.put("DOC", "`DOC` was replaced by [`INFO`](https://www.warp10.io/doc/INFO)");
    removedFunctions.put("DOCMODE", "`DOCMODE` was replaced by [`INFOMODE`](https://www.warp10.io/doc/INFOMODE)");
    removedFunctions.put("FROMBITS", "`FROMBITS` was replaced by [`DOUBLEBITS->`](https://www.warp10.io/doc/AIt3IpK1I3K1HKGI)");
    removedFunctions.put("HLOCATE", "Warp 10 3.x does not rely on Hbase anymore.");
    removedFunctions.put("ISAUTHENTICATED", "All the functions that needed authentication now needs capabilities"); // TODO(ppn): Add link to capabilities page
    removedFunctions.put("LEVELDBSECRET", "Secret was replaced by leveldb or leveldb.admin capability"); // TODO(ppn): Add link to capabilities page
    removedFunctions.put("MACROCONFIGSECRET", "Secret was replaced by setmacroconfig capability"); // TODO(ppn): Add link to capabilities page
    removedFunctions.put("MAXURLFETCHCOUNT", "`URLFETCH` was removed, see [`HTTP`](https://www.warp10.io/doc/HTTP)");
    removedFunctions.put("MAXURLFETCHSIZE", "`URLFETCH` was removed, see [`HTTP`](https://www.warp10.io/doc/HTTP)");
    removedFunctions.put("STACKPSSECRET", "Secret was replaced by stackps capability"); // TODO(ppn): Add link to capabilities page
    removedFunctions.put("TOBITS", "`TOBITS` was replaced by [`DOUBLEBITS->`](https://www.warp10.io/doc/AIt3IpK1I3K1HKGI) or [`->FLOATBITS`](https://www.warp10.io/doc/AIt5I3x0K388K4B)");
    removedFunctions.put("TOKENSECRET", "Token management secret was replaced by tokengen or tokendump capabilities"); // TODO(ppn): Add link to capabilities page
    removedFunctions.put("UDF", "");
    removedFunctions.put("URLFETCH", "`URLFETCH` was removed, see [`HTTP`](https://www.warp10.io/doc/HTTP)");
    removedFunctions.put("WEBCALL", "`WEBCALL` was removed, see [`HTTP`](https://www.warp10.io/doc/HTTP)");

    HashMap<String, String> behaviorChangedFunctions = new HashMap<>();
    behaviorChangedFunctions.put("CALL", "[`CALL`](https://www.warp10.io/doc/CALL) has now a default 10s timeout. This can be changed by `warpscript.call.maxwait` configuration or customized per user with `call.maxwait` capability.");
    behaviorChangedFunctions.put("HTTP", "[`HTTP`](https://www.warp10.io/doc/HTTP) has now a default 60s timeout. This can be changed by `warpscript.http.maxtimeout` configuration or customized per user with `http.maxtimeout` capability.");
    behaviorChangedFunctions.put("MUTEX", "[`MUTEX`](https://www.warp10.io/doc/MUTEX) has now a default 5s timeout. This can be changed by `mutex.maxwait` configuration or customized per user with `mutex.maxwait` capability.");

    HashMap<String, String> capabilityNeededFunctions = new HashMap<>();
    capabilityNeededFunctions.put("HTTP", "[`HTTP`](https://www.warp10.io/doc/HTTP) depends on the following capabilities: `http`, `http.requests`, `http.size`, `http.chunksize`, `http.maxtimeout`.");
    capabilityNeededFunctions.put("LIMIT", "[`LIMIT`](https://www.warp10.io/doc/LIMIT) depends on the following capabilities: `limits`, or `limit`.");
    capabilityNeededFunctions.put("MAXBUCKETS", "[`MAXBUCKETS`](https://www.warp10.io/doc/MAXBUCKETS) depends on the following capabilities: `limits`, or `maxbuckets`.");
    capabilityNeededFunctions.put("MAXDEPTH", "[`MAXDEPTH`](https://www.warp10.io/doc/MAXDEPTH) depends on the following capabilities: `limits`, or `maxdepth`.");
    capabilityNeededFunctions.put("MAXGEOCELLS", "[`MAXGEOCELLS`](https://www.warp10.io/doc/MAXGEOCELLS) depends on the following capabilities: `limits`, or `maxgeocells`.");
    capabilityNeededFunctions.put("MAXGTS", "[`MAXGTS`](https://www.warp10.io/doc/MAXGTS) depends on the following capabilities: `limits`, or `maxgts`.");
    capabilityNeededFunctions.put("MAXLOOP", "[`MAXLOOP`](https://www.warp10.io/doc/MAXLOOP) depends on the following capabilities: `limits`, or `maxloop`.");
    capabilityNeededFunctions.put("MAXOPS", "[`MAXOPS`](https://www.warp10.io/doc/MAXOPS) depends on the following capabilities: `limits`, or `maxops`.");
    capabilityNeededFunctions.put("MAXPIXELS", "[`MAXPIXELS`](https://www.warp10.io/doc/MAXPIXELS) depends on the following capabilities: `limits`, or `maxpixels`.");
    capabilityNeededFunctions.put("MAXRECURSION", "[`MAXRECURSION`](https://www.warp10.io/doc/MAXRECURSION) depends on the following capabilities: `limits`, or `maxrecursion`.");
    capabilityNeededFunctions.put("MAXSYMBOLS", "[`MAXSYMBOLS`](https://www.warp10.io/doc/MAXSYMBOLS) depends on the following capabilities: `limits`, or `maxsymbols`.");
    capabilityNeededFunctions.put("REPORT", "[`REPORT`](https://www.warp10.io/doc/REPORT) depends on the following capabilities: `report`.");
    capabilityNeededFunctions.put("REXEC", "[`REXEC`](https://www.warp10.io/doc/REXEC) depends on the following capabilities: `rexec`.");
    capabilityNeededFunctions.put("REXECZ", "[`REXECZ`](https://www.warp10.io/doc/REXECZ) depends on the following capabilities: `rexec`.");
    capabilityNeededFunctions.put("SETMACROCONFIG", "[`SETMACROCONFIG`](https://www.warp10.io/doc/SETMACROCONFIG) depends on the following capabilities: `setmacroconfig`.");
    capabilityNeededFunctions.put("WSKILL", "[`WSKILL`](https://www.warp10.io/doc/WSKILL) depends on the following capabilities: `stackps`.");
    capabilityNeededFunctions.put("WSPS", "[`WSPS`](https://www.warp10.io/doc/WSPS) depends on the following capabilities: `stackps`.");
    capabilityNeededFunctions.put("LOGMSG", "[`LOGMSG`](https://www.warp10.io/doc/LOGMSG) depends on the following capabilities: `debug`.");
    capabilityNeededFunctions.put("NOLOG", "[`NOLOG`](https://www.warp10.io/doc/NOLOG) depends on the following capabilities: `debug`.");
    capabilityNeededFunctions.put("STDERR", "[`STDERR`](https://www.warp10.io/doc/STDERR) depends on the following capabilities: `debug`.");
    capabilityNeededFunctions.put("STDOUT", "[`STDOUT`](https://www.warp10.io/doc/STDOUT) depends on the following capabilities: `debug`.");
    capabilityNeededFunctions.put("FUNCTIONS", "[`FUNCTIONS`](https://www.warp10.io/doc/FUNCTIONS) depends on the following capabilities: `inventory`.");
    capabilityNeededFunctions.put("SENSISION.EVENT", "[`SENSISION.EVENT`](https://www.warp10.io/doc/SENSISION.EVENT) depends on the following capabilities: `sensision.write`.");
    capabilityNeededFunctions.put("SENSISION.GET", "[`SENSISION.GET`](https://www.warp10.io/doc/SENSISION.GET) depends on the following capabilities: `sensision.read`.");
    capabilityNeededFunctions.put("SENSISION.SET", "[`SENSISION.SET`](https://www.warp10.io/doc/SENSISION.SET) depends on the following capabilities: `sensision.write`.");
    capabilityNeededFunctions.put("SENSISION.UPDATE", "[`SENSISION.UPDATE`](https://www.warp10.io/doc/SENSISION.UPDATE) depends on the following capabilities: `sensision.write`.");
    capabilityNeededFunctions.put("SENSISION.DUMP", "[`SENSISION.DUMP`](https://www.warp10.io/doc/SENSISION.DUMP) depends on the following capabilities: `sensision.read`.");
    capabilityNeededFunctions.put("SENSISION.DUMPEVENTS", "[`SENSISION.DUMPEVENTS`](https://www.warp10.io/doc/SENSISION.DUMPEVENTS) depends on the following capabilities: `sensision.read`.");
    capabilityNeededFunctions.put("MUTEX", "[`MUTEX`](https://www.warp10.io/doc/MUTEX) depends on the following capabilities: `mutex`, `mutex.maxwait`.");
    capabilityNeededFunctions.put("SHMLOAD", "[`SHMLOAD`](https://www.warp10.io/doc/SHMLOAD) depends on the following capabilities: `shmload`.");
    capabilityNeededFunctions.put("SHMSTORE", "[`SHMSTORE`](https://www.warp10.io/doc/SHMSTORE) depends on the following capabilities: `shmstore`.");
    capabilityNeededFunctions.put("TOKENDUMP", "[`TOKENDUMP`](https://www.warp10.io/doc/TOKENDUMP) depends on the following capabilities: `tokendump`.");
    capabilityNeededFunctions.put("TOKENGEN", "[`TOKENGEN`](https://www.warp10.io/doc/TOKENGEN) depends on the following capabilities: `tokengen`.");
    capabilityNeededFunctions.put("DELETEOFF", "[`DELETEOFF`](https://www.warp10.io/doc/DELETEOFF) depends on the following capabilities: `manager`.");
    capabilityNeededFunctions.put("DELETEON", "[`DELETEON`](https://www.warp10.io/doc/DELETEON) depends on the following capabilities: `manager`.");
    capabilityNeededFunctions.put("METAOFF", "[`METAOFF`](https://www.warp10.io/doc/METAOFF) depends on the following capabilities: `manager`.");
    capabilityNeededFunctions.put("METAON", "[`METAON`](https://www.warp10.io/doc/METAON) depends on the following capabilities: `manager`.");
    capabilityNeededFunctions.put("UPDATEOFF", "[`UPDATEOFF`](https://www.warp10.io/doc/UPDATEOFF) depends on the following capabilities: `manager`.");
    capabilityNeededFunctions.put("UPDATEON", "[`UPDATEON`](https://www.warp10.io/doc/UPDATEON) depends on the following capabilities: `manager`.");
    capabilityNeededFunctions.put("WF.ADDREPO", "[`WF.ADDREPO`](https://www.warp10.io/doc/WF.ADDREPO) depends on the following capabilities: `wfset`.");
    capabilityNeededFunctions.put("WF.GETREPOS", "[`WF.GETREPOS`](https://www.warp10.io/doc/WF.GETREPOS) depends on the following capabilities: `wfget`.");
    capabilityNeededFunctions.put("WF.SETREPOS", "[`WF.SETREPOS`](https://www.warp10.io/doc/WF.SETREPOS) depends on the following capabilities: `wfget`.");
    capabilityNeededFunctions.put("MAXJSON", "[`MAXJSON`](https://www.warp10.io/doc/MAXJSON) depends on the following capabilities: `limits`, or `maxjson`.");
    capabilityNeededFunctions.put("LEVELDBCLOSE", "[`LEVELDBCLOSE`](https://www.warp10.io/doc/LEVELDBCLOSE) depends on the following capabilities: `leveldb.admin`, or `leveldb.close`.");
    capabilityNeededFunctions.put("LEVELDBOPEN", "[`LEVELDBOPEN`](https://www.warp10.io/doc/LEVELDBOPEN) depends on the following capabilities: `leveldb.admin`, or `leveldb.open`.");
    capabilityNeededFunctions.put("LEVELDBSNAPSHOT", "[`LEVELDBSNAPSHOT`](https://www.warp10.io/doc/LEVELDBSNAPSHOT) depends on the following capabilities: `leveldb.admin`, or `leveldb.snapshot`.");
    capabilityNeededFunctions.put("SSTFIND", "[`SSTFIND`](https://www.warp10.io/doc/SSTFIND) depends on the following capabilities: `leveldb`, or `leveldb.find`.");
    capabilityNeededFunctions.put("SSTINFO", "[`SSTINFO`](https://www.warp10.io/doc/SSTINFO) depends on the following capabilities: `leveldb`, or `leveldb.info`.");
    capabilityNeededFunctions.put("SSTPURGE", "[`SSTPURGE`](https://www.warp10.io/doc/SSTPURGE) depends on the following capabilities: `leveldb.admin`, or `leveldb.purge`.");
    capabilityNeededFunctions.put("SSTREPORT", "[`SSTREPORT`](https://www.warp10.io/doc/SSTREPORT) depends on the following capabilities: `leveldb`, or `leveldb.report`.");
    capabilityNeededFunctions.put("SSTTIMESTAMP", "[`SSTTIMESTAMP`](https://www.warp10.io/doc/SSTTIMESTAMP) depends on the following capabilities: `leveldb`, or `leveldb.timestamp`.");
    capabilityNeededFunctions.put("HFDUMP", "[`HFDUMP`](https://www.warp10.io/doc/HFDUMP) depends on the following capabilities: `hfdump`.");
    capabilityNeededFunctions.put("HFINDEX", "[`HFINDEX`](https://www.warp10.io/doc/HFINDEX) depends on the following capabilities: `hfindex`.");
    capabilityNeededFunctions.put("HFINFO", "[`HFINFO`](https://www.warp10.io/doc/HFINFO) depends on the following capabilities: `hfinfo`.");
    capabilityNeededFunctions.put("HFRESCAN", "[`HFRESCAN`](https://www.warp10.io/doc/HFRESCAN) depends on the following capabilities: `hfrescan`.");
    capabilityNeededFunctions.put("HFCAT", "[`HFCAT`](https://www.warp10.io/doc/HFCAT) depends on the following capabilities: `hfcat`.");
    capabilityNeededFunctions.put("HFOPEN", "[`HFOPEN`](https://www.warp10.io/doc/HFOPEN) depends on the following capabilities: `hfopen`.");
    capabilityNeededFunctions.put("HFCLOSE", "[`HFCLOSE`](https://www.warp10.io/doc/HFCLOSE) depends on the following capabilities: `hfclose`.");
    capabilityNeededFunctions.put("RUNNERIN", "[`RUNNERIN`](https://www.warp10.io/doc/RUNNERIN) depends on the following capabilities: `runner.reschedule.min.period`.");
    capabilityNeededFunctions.put("RUNNERAT", "[`RUNNERAT`](https://www.warp10.io/doc/RUNNERAT) depends on the following capabilities: `runner.reschedule.min.period`.");
    capabilityNeededFunctions.put("SLEEP", "[`SLEEP`](https://www.warp10.io/doc/SLEEP) depends on the following capabilities: `sleep.maxtime`.");

    WarpScriptAudit wsa = new WarpScriptAudit();
    List sts = wsa.parseWarpScriptStatements(ws);
    Set<String> unknownStatements = new HashSet<>();

    for (int i = 0; i < sts.size(); i++) {
      Object s = sts.get(i);
      if (s instanceof Statement) {
        Statement st = (Statement) s;
        // Removed functions
        if (st.type == STATEMENT_TYPE.UNKNOWN) {
          if (removedFunctions.containsKey(st.statement)) {
            issues.add(new Issue(ISSUE_GRAVITY.ERROR, st.statement + " function was removed in 3.x", st.lineNumber, st.statementNumber, removedFunctions.get(st.statement)));
          } else {
            unknownStatements.add(st.statement);
          }
        }
        // Behavior changed
        if (st.type == STATEMENT_TYPE.FUNCTION_CALL && st.statement.equals("MAP")) {
          // there could be an issue with "ticks". Look backward if there is a map construction and "ticks"... 
          // if so, warning. if not sure, info.
          if (backwardStatementSearch(sts, "}", i, 2, STATEMENT_TYPE.FUNCTION_CALL) > 0) {
            boolean foundTicks = backwardStatementSearch(sts, "ticks", i, 500, STATEMENT_TYPE.STRING) > 0;
            issues.add(new Issue(foundTicks ? ISSUE_GRAVITY.WARNING : ISSUE_GRAVITY.INFO, st.statement + " behavior changed", st.lineNumber, st.statementNumber, "MAP ticks parameter behavior was fixed in 3.x. Check [`MAP`](https://www.warp10.io/doc/MAP"));
          }
        }
        if (st.type == STATEMENT_TYPE.FUNCTION_CALL && behaviorChangedFunctions.containsKey(st.statement)) {
          issues.add(new Issue(ISSUE_GRAVITY.WARNING, st.statement + " behavior changed", st.lineNumber, st.statementNumber, behaviorChangedFunctions.get(st.statement)));
        }
        // Capability needed warning, if CAPADD not detected before, or info, if detected.
        if (st.type == STATEMENT_TYPE.FUNCTION_CALL && capabilityNeededFunctions.containsKey(st.statement)) {
          // look for CAPADD or CAPCHECK or CAPGET before
          boolean capManaged = backwardStatementSearch(sts, "CAPADD", i, Integer.MAX_VALUE, STATEMENT_TYPE.FUNCTION_CALL) > 0 ||
              backwardStatementSearch(sts, "CAPCHECK", i, Integer.MAX_VALUE, STATEMENT_TYPE.FUNCTION_CALL) > 0 ||
              backwardStatementSearch(sts, "CAPGET", i, Integer.MAX_VALUE, STATEMENT_TYPE.FUNCTION_CALL) > 0;
          if (capManaged) {
            issues.add(new Issue(ISSUE_GRAVITY.INFO, st.statement + " needs a capability", st.lineNumber, st.statementNumber, capabilityNeededFunctions.get(st.statement)));
          } else {
            issues.add(new Issue(ISSUE_GRAVITY.WARNING, st.statement + " needs a capability, and there is no CAPADD or CAPCHECK or CAPGET before", st.lineNumber, st.statementNumber, capabilityNeededFunctions.get(st.statement)));
          }
        }
      }
    }
    // In case of unknown statements, add one more issue
    if (unknownStatements.size() > 0) {
      issues.add(new Issue(ISSUE_GRAVITY.WARNING, "Unknown statement found", 0L, 0, "Unknown functions found. If these are linked to an extension, you must pass the full Warp 10 configuration to the audit tool. List of unknown statements:" + unknownStatements));
    }

    return issues;
  }

  public static int backwardStatementSearch(List statementList, String statement, int start, int searchLength, STATEMENT_TYPE type) {
    int end = statementList.size() - searchLength;
    end = end < 0 ? 0 : end;
    for (int i = start; i > end; i--) {
      Object s = statementList.get(i);
      if (s instanceof Statement) {
        if (((Statement) s).statement.equals(statement) && ((Statement) s).type == type) {
          return i;
        }
      }
    }
    return -1;
  }


  public static void issuesToMarkDown(List<Issue> issues, StringBuilder sb) {
    sb.append("|Type|Line|Issue|Advice|\n");
    sb.append("|---|---|---|---|\n");
    issues.sort(new Comparator<Issue>() {
      @Override
      public int compare(Issue o1, Issue o2) {
        return o1.gravity.compareTo(o2.gravity);
      }
    });
    for (Issue i: issues) {
      sb.append("|" + i.gravity + "|" + i.lineNumber + "|" + i.issue + "|" + i.advice + "|\n");
    }
  }

  public static String auditFileToMarkdown(Path p, String auditTask) {
    String ws = "";
    List<Issue> issues = new ArrayList<>();
    try {
      ws = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
      if (auditTask.equalsIgnoreCase(AUDIT_TASK_3_X_MIGRATION)) {
        issues = migration3xAudit(ws);
      } else if (auditTask.equalsIgnoreCase(AUDIT_TASK_PARSING)) {
        issues = parsingAudit(ws);
      }
    } catch (IOException e) {
      issues.add(new Issue(ISSUE_GRAVITY.ERROR, "Cannot read file " + p.toString(), 0L, 0));
    }

    StringBuilder sb = new StringBuilder();
    sb.append("# " + p.toString() + "\n\n");

    if (issues.size() == 0) {
      sb.append("No issue found for Warp 10 3.x migration\n\n");
    } else {
      issuesToMarkDown(issues, sb);
    }
    sb.append("\n\n");

    return sb.toString();
  }

  public static List<Issue> auditFileToJson(Path p, String auditTask) {
    String ws = "";
    List<Issue> issues = new ArrayList<>();
    try {
      ws = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
      if (auditTask.equalsIgnoreCase(AUDIT_TASK_3_X_MIGRATION)) {
        issues = migration3xAudit(ws);
      } else if (auditTask.equalsIgnoreCase(AUDIT_TASK_PARSING)) {
        issues = parsingAudit(ws);
      }
    } catch (IOException e) {
      issues.add(new Issue(ISSUE_GRAVITY.ERROR, "Cannot read file " + p.toString(), 0L, 0));
    }
    return issues;
  }

  public static String auditWsToJon(String ws, String auditTask) throws IOException {
    List<Issue> issues = new ArrayList<>();
    if (auditTask.equalsIgnoreCase(AUDIT_TASK_3_X_MIGRATION)) {
      issues = migration3xAudit(ws);
    } else if (auditTask.equalsIgnoreCase(AUDIT_TASK_PARSING)) {
      issues = parsingAudit(ws);
    } else {
      issues.add(new Issue(ISSUE_GRAVITY.ERROR, "Audit Task " + auditTask + " is not supported", 0L, 0,
          "Supported tasks are: " + AUDIT_TASK_3_X_MIGRATION + ", " + AUDIT_TASK_PARSING));
    }
    return JsonUtils.objectToJson(issues, true);
  }

  /**
   * Command line tool to run an audit on files, or directories. Default output format is markdown.
   * command example:
   * java -cp "/opt/warp10/bin/warp10-3.0.0.jar:/opt/warp10/lib/*"
   * -Dwarp10.config="/opt/warp10/etc/conf.d/00-secrets.conf /opt/warp10/etc/conf.d/01-conf-standalone.conf  ... ..."
   * -Dwarpaudit.format=json
   * io.warp10.script.WarpScriptAudit audit3x ./tests-staticAnalyse/test1.mc2 ./tests-staticAnalyse/subdir/
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.out.println("No arguments, stopping.");
      System.out.println("First argument must be the audit task name, such as 'audit3x'.");
      System.out.println("Following arguments could be a list of files or directories.");
      System.exit(0);
    }

    String auditTask = args[0];

    //
    // Initialize WarpConfig
    //
    System.setProperty(Configuration.WARP10_QUIET, "true");
    if (null == System.getProperty(Configuration.WARP_TIME_UNITS)) {
      System.setProperty(Configuration.WARP_TIME_UNITS, "us");
    }
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

    // Build a list of files to process. Arguments could either be file path, or directory path.
    List<Path> listOfFiles = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String dirOrFile = args[i];
      File file = new File(dirOrFile);
      if (file.isDirectory()) {
        try {
          Iterator<Path> iter = null;
          try {
            iter =
                Files.find(Paths.get(dirOrFile),
                        Integer.MAX_VALUE,
                        (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.toString().endsWith(".mc2"))
                    .iterator();
          } catch (NoSuchFileException nsfe) {
            System.err.println("> **Error** cannot find *" + dirOrFile + "*");
          }
          while (null != iter && iter.hasNext()) {
            Path p = iter.next();
            listOfFiles.add(p);
          }
        } catch (Throwable t) {
          System.err.println("> **Error** while loading *" + dirOrFile + "*");
        }
      } else if (file.isFile() && dirOrFile.endsWith(".mc2")) {
        listOfFiles.add(file.toPath());
      }
    }

    // Defaults to Markdown
    boolean json = "json".equals(WarpConfig.getProperty(WARPAUDIT_FORMAT));


    WarpScriptAudit wsa = new WarpScriptAudit();

    if (auditTask.equalsIgnoreCase(AUDIT_TASK_3_X_MIGRATION) || auditTask.equalsIgnoreCase(AUDIT_TASK_PARSING)) {
      if (json) {
        Map<String, List> issuesByFile = new LinkedHashMap<>();
        for (Path p: listOfFiles) {
          issuesByFile.put(p.toString(), auditFileToJson(p, auditTask));
        }
        System.out.println(JsonUtils.objectToJson(issuesByFile, true, true));
      } else {
        // markdown
        for (Path p: listOfFiles) {
          System.out.println(auditFileToMarkdown(p, auditTask));
        }
      }
    } else {
      System.out.println("Unknown audit task name: '" + auditTask + "'");
    }


    System.exit(0);
  }


}
