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

/**
 * Stores a statement, a type, and its position.
 * This class is json serializable, snapshotable and, is a WarpScriptStackFunction.
 * Type is defined by {@link WarpScriptTraceableStatement.STATEMENT_TYPE}
 */
public class WarpScriptTraceableStatement implements WarpScriptStackFunction {

  public enum STATEMENT_TYPE {
    FUNCTION_CALL,    // function present in WarpScriptLib
    WS_EXCEPTION,     // in this case, statementObject will be a string that contains the exception message 
    WS_EARLY_BINDING, // managed as a simple load for the moment
    WS_LOAD,          // load action is stored as one statement, to store the right statement position
    WS_RUN,           // run action is stored as one statement, to store the right statement position
    UNKNOWN           // syntax error (missing space, [5 is not a function), function call to a REDEF function, or unknown extension.
  }

  public WarpScriptTraceableStatement.STATEMENT_TYPE type;
  public String statement;
  public Long lineNumber;      // first line is numbered 1
  public int positionNumber;   // first statement in line is numbered 0
  private Object statementObject;

  public WarpScriptTraceableStatement(WarpScriptTraceableStatement.STATEMENT_TYPE type, Object statementObject, String statement, Long lineNumber, int statementNumber) {
    this.type = type;
    this.statement = statement;
    this.lineNumber = lineNumber;
    this.positionNumber = statementNumber;
    this.statementObject = statementObject;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (type == STATEMENT_TYPE.FUNCTION_CALL) {
      if (statementObject instanceof WarpScriptStackFunction) {
        try {
          ((WarpScriptStackFunction) statementObject).apply(stack);
        } catch (WarpScriptException e) {
          throw new WarpScriptException("Exception" + formatWSAuditPosition() + ".", e);
        }
      } else {
        stack.push(statementObject);  // aggregators for example
      }
    } else if (type == STATEMENT_TYPE.UNKNOWN) {
      throw new WarpScriptException("Unknown function " + statement + formatWSAuditPosition() + ".");
    } else if (type == STATEMENT_TYPE.WS_EXCEPTION) {
      throw new WarpScriptException(statement + formatWSAuditPosition());
    } else if (type == STATEMENT_TYPE.WS_EARLY_BINDING) {
      throw new WarpScriptException(WarpScriptLib.WSAUDITMODE + " does not support indirect binding. (" + statement + ")" + formatWSAuditPosition() + ".");
    } else if (type == STATEMENT_TYPE.WS_LOAD) {
      Object o = stack.load(statement.substring(1));
      if (null == o) {
        if (!stack.getSymbolTable().containsKey(statement.substring(1))) {
          throw new WarpScriptException("Unknown symbol '" + statement.substring(1) + "'" + formatWSAuditPosition() + ".");
        }
      }
      stack.push(o);
    } else if (type == STATEMENT_TYPE.WS_RUN) {
      try {
        stack.run(statement.substring(1));
      } catch (WarpScriptException e) {
        throw new WarpScriptException("Exception at line " + lineNumber + ", position " + positionNumber + ".", e);
      }
    }
    return stack;
  }

  /**
   * fix the format of position, in order to parse the exception message easily in other tools.
   *
   * @return " (line x, position y)", where x and y are numbers.
   */
  public String formatWSAuditPosition() {
    return " (line " + lineNumber + ", position " + positionNumber + ")";
  }

  // used for SNAPSHOT
  @Override
  public String toString() {
    switch (type) {
      case FUNCTION_CALL:  // valid function
        if (statementObject instanceof WarpScriptStackFunction) {
          return StackUtils.toString(statement) + " " + WarpScriptLib.FUNCREF;
        } else {
          return StackUtils.toString(statement);
        }
      case UNKNOWN:  // invalid function 
        return StackUtils.toString("Unknown function (" + statement + ")" + formatWSAuditPosition() + ".") + " " + WarpScriptLib.MSGFAIL;
      case WS_EXCEPTION:  // other parsing exception
        return StackUtils.toString("Exception (" + statement + ")" + formatWSAuditPosition() + ".") + " " + WarpScriptLib.MSGFAIL;
      case WS_EARLY_BINDING:
        return StackUtils.toString(WarpScriptLib.WSAUDITMODE + " does not support indirect binding. (" + statement + ")" + formatWSAuditPosition() + ".") + " " + WarpScriptLib.MSGFAIL;
      case WS_LOAD:
        return StackUtils.toString(statement.substring(1)) + " " + StackUtils.toString(WarpScriptLib.LOAD) + " " + WarpScriptLib.FUNCREF;
      case WS_RUN:
        return StackUtils.toString(statement.substring(1)) + " " + StackUtils.toString(WarpScriptLib.RUN) + " " + WarpScriptLib.FUNCREF;
      default:
        return "";
    }

  }

}
