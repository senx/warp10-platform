//
//   Copyright 2024  SenX S.A.S.
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

package io.warp10.script.functions;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Pushes onto the stack a random BigInteger (as a BigDecimal) probably prime from the seeded PRNG
 */
public class BDPROBABLEPRIME extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean seeded;

  private static final SecureRandom sr = new SecureRandom();

  public BDPROBABLEPRIME(String name, boolean seeded) {
    super(name);
    this.seeded = seeded;
  }


  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Random prng = sr;

    if (seeded) {
      prng = (Random) stack.getAttribute(PRNG.ATTRIBUTE_SEEDED_PRNG);
      if (null == prng) {
        throw new WarpScriptException(getName() + " seeded PRNG was not initialized.");
      }
    }


    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a LONG number of bits to generate.");
    }

    long numbits = ((Long) top).longValue();

    if (numbits < 0 || numbits > Integer.MAX_VALUE) {
      throw new WarpScriptException(getName() + " invalid number of bits, MUST be between 0 and " + Integer.MAX_VALUE + ".");
    }

    BigInteger bi = BigInteger.probablePrime((int) numbits, prng);

    stack.push(TOBD.toBigDecimal(getName(), bi));

    return stack;
  }
}
