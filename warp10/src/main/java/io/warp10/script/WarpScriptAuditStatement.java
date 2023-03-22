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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stores a statement, a type, and its position.
 * This class is json serializable, snapshotable and, is a WarpScriptStackFunction.
 * Type is defined by {@link WarpScriptAuditStatement.STATEMENT_TYPE}
 */
public class WarpScriptAuditStatement implements WarpScriptStackFunction {

  public static final String KEY_TYPE = "type";
  public static final String KEY_LINE = "line";
  public static final String KEY_POSITION = "position";
  public static final String KEY_STATEMENT = "statement";

  public enum STATEMENT_TYPE {
    FUNCTION_CALL,    // function present in WarpScriptLib
    WS_EXCEPTION,     // in this case, statementObject will be a string that contains the exception message 
    WS_EARLY_BINDING, // managed as a simple load for the moment
    WS_LOAD,          // load action is stored as one statement, to store the right statement position
    WS_RUN,           // run action is stored as one statement, to store the right statement position
    UNKNOWN           // syntax error (missing space, [5 is not a function), function call to a REDEF function, or unknown extension.
  }

  public STATEMENT_TYPE type;
  public String statement;
  public long lineNumber;      // first line is numbered 1
  public int positionNumber;   // first statement in line is numbered 0
  public Object statementObject;

  public WarpScriptAuditStatement(STATEMENT_TYPE type, Object statementObject, String statement, Long lineNumber, int statementNumber) {
    this.type = type;
    this.statement = statement;
    this.lineNumber = lineNumber;
    this.positionNumber = statementNumber;
    this.statementObject = statementObject;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    switch (type) {
      case FUNCTION_CALL:
        if (statementObject instanceof WarpScriptStackFunction) {
          try {
            ((WarpScriptStackFunction) statementObject).apply(stack);
          } catch (WarpScriptException e) {
            throw new WarpScriptException("Exception" + formatPosition() + ".", e);
          }
        } else {
          stack.push(statementObject);  // aggregators for example
        }
        break;
      case UNKNOWN:
        throw new WarpScriptException("Unknown function " + statement + formatPosition() + ".");
      case WS_EXCEPTION:
        throw new WarpScriptException(statement + formatPosition());
      case WS_EARLY_BINDING:
        throw new WarpScriptException(WarpScriptLib.WSAUDITMODE + " does not support early binding. (" + statement + ")" + formatPosition() + ".");
      case WS_LOAD:
        Object o = stack.load(statement.substring(1));
        if (null == o) {
          if (!stack.getSymbolTable().containsKey(statement.substring(1))) {
            throw new WarpScriptException("Unknown symbol '" + statement.substring(1) + "'" + formatPosition() + ".");
          }
        }
        stack.push(o);
        break;
      case WS_RUN:
        try {
          stack.run(statement.substring(1));
        } catch (WarpScriptException e) {
          throw new WarpScriptException("Exception" + formatPosition(), e);
        }
        break;
    }
    return stack;
  }

  /**
   * fix the format of position, in order to parse the exception message easily in other tools.
   *
   * @return " (line x, position y)", where x and y are numbers.
   */
  public String formatPosition() {
    return " (line " + lineNumber + ", position " + positionNumber + ")";
  }

  /**
   * Turns WarpScriptAuditStatement into a MAP that can be used in WarpScript
   * @return map with the same keys as the json WarpScriptAuditStatement serializer.
   */
  public Map toMap() {
    Map m = new LinkedHashMap();
    m.put(KEY_TYPE,type.name());
    m.put(KEY_LINE,lineNumber);
    m.put(KEY_POSITION,((Integer)positionNumber).longValue()); // WarpScript knows LONG 
    m.put(KEY_STATEMENT,statement);
    return m;
  } 
  
  // used for SNAPSHOT
  @Override
  public String toString() {
    switch (type) {
      case FUNCTION_CALL:
        return StackUtils.toString(statement) + " " + WarpScriptLib.FUNCREF;
      case UNKNOWN:  // invalid function 
        return StackUtils.toString("Unknown function (" + statement + ")" + formatPosition() + ".") + " " + WarpScriptLib.MSGFAIL;
      case WS_EXCEPTION:  // other parsing exception
        return StackUtils.toString("Exception (" + statement + ")" + formatPosition() + ".") + " " + WarpScriptLib.MSGFAIL;
      case WS_EARLY_BINDING:  // early binding execution will fail
        return StackUtils.toString(WarpScriptLib.WSAUDITMODE + " does not support early binding. (" + statement + ")" + formatPosition() + ".") + " " + WarpScriptLib.MSGFAIL;
      case WS_LOAD:
        return StackUtils.toString(statement.substring(1)) + " " + WarpScriptLib.LOAD;
      case WS_RUN:
        return StackUtils.toString(statement.substring(1)) + " " + WarpScriptLib.RUN;
      default:
        return "";
    }

  }

}
