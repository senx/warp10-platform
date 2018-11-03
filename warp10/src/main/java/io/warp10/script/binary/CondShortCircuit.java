//
//   Copyright 2018  SenX S.A.S.
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

package io.warp10.script.binary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;

import java.util.List;

/**
 * Conditional boolean operation taking either:
 * - two operands on top of the stack.
 * - a list of booleans or boolean-returning-macros.
 * This class implements short-circuit evaluation (https://en.wikipedia.org/wiki/Short-circuit_evaluation).
 */
public abstract class CondShortCircuit extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  protected final boolean triggerValue;

  public abstract boolean operator(boolean bool1, boolean bool2);

  public CondShortCircuit(String name, boolean triggerValue) {
    super(name);
    this.triggerValue = triggerValue;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String exceptionMessage = getName() + " can only operate on two boolean values or a list of booleans or macros, each macro putting a single boolean on top of the stack.";

    Object top = stack.pop();

    if (top instanceof List) {
      // List version: check each element, one after another, exiting when triggerValue() is found. In case of early
      // exiting, triggerValue() is putted on top of the stack.
      for (Object operand : (List) top) {
        // If a macro is found, execute it and use the result as the operand.
        if (WarpScriptLib.isMacro(operand)) {
          stack.exec((WarpScriptStack.Macro) operand);
          operand = stack.pop();
        }

        if (operand instanceof Boolean) {
          // Short-circuit evaluation: found an early exit case.
          if (triggerValue == (boolean) operand) {
            stack.push(triggerValue);
            return stack;
          }
        } else {
          throw new WarpScriptException(exceptionMessage);
        }
      }

      // No short-circuit found
      stack.push(!triggerValue);
      return stack;
    } else {
      // Simple case: both operands are booleans, do a || and push to the stack.
      Object operand2 = top;
      Object operand1 = stack.pop();

      if (operand2 instanceof Boolean && operand1 instanceof Boolean) {
        stack.push(operator((boolean) operand1, (boolean) operand2));
        return stack;
      } else {
        throw new WarpScriptException(exceptionMessage);
      }
    }
  }
}
