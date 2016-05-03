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

package io.warp10.script.binary;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

/**
 * Adds the two operands on top of the stack
 */
public class ADD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ADD(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Number && op1 instanceof Number) {
      if (op1 instanceof Double || op2 instanceof Double) {
        stack.push(((Number) op1).doubleValue() + ((Number) op2).doubleValue());        
      } else {
        stack.push(((Number) op1).longValue() + ((Number) op2).longValue());
      }
    } else if (op2 instanceof String && op1 instanceof String) {
      stack.push(op1.toString() + op2.toString());
    } else if (op1 instanceof List) {
      List<Object> l = new ArrayList<Object>();
      l.addAll((List) op1);
      l.add(op2);
      stack.push(l);
    } else if (op1 instanceof Set) {
      Set<Object> s = new HashSet<Object>();
      s.addAll((Set) op1);
      s.add(op2);
      stack.push(s);
    } else if (op1 instanceof Macro && op2 instanceof Macro) {
      Macro macro = new Macro();
      macro.addAll((Macro) op1);
      macro.addAll((Macro) op2);
      stack.push(macro);
    } else if (op1 instanceof RealMatrix && op2 instanceof RealMatrix) {
      stack.push(((RealMatrix) op1).add((RealMatrix) op2));
    } else if (op1 instanceof RealMatrix && op2 instanceof Number) {
      stack.push(((RealMatrix) op1).scalarAdd(((Number) op2).doubleValue()));
    } else if (op2 instanceof RealMatrix && op1 instanceof Number) {
      stack.push(((RealMatrix) op2).scalarAdd(((Number) op1).doubleValue()));
    } else if (op1 instanceof RealVector && op2 instanceof RealVector) {
      stack.push(((RealVector) op1).add((RealVector) op2));
    } else if (op1 instanceof RealVector && op2 instanceof Number) {
      stack.push(((RealVector) op1).mapAdd(((Number) op2).doubleValue()));
    } else if (op2 instanceof RealVector && op1 instanceof Number) {
      stack.push(((RealVector) op2).mapAdd(((Number) op1).doubleValue()));
    } else {
      throw new WarpScriptException(getName() + " can only operate on numeric, string, lists, matrices, vectors and macro values.");
    }
    
    return stack;
  }
}
