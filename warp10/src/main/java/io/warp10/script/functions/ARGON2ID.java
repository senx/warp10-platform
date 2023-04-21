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

package io.warp10.script.functions;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class ARGON2ID extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String KEY_ITERATIONS = "iterations";
  private static final String KEY_MEMORY = "memory";
  private static final String KEY_PARALLELISM = "parallelism";
  private static final String KEY_SECRET = "secret";
  private static final String KEY_SALT = "salt";
  private static final String KEY_ADDITIONAL = "additional";
  private static final String KEY_SIZE = "size";
  private static final String KEY_PASSWORD = "password";

  private static final String CAP_ARGON2ID_MAXITER = "argon2id.maxiter";
  private static final String CAP_ARGON2ID_MAXPAR = "argon2id.maxpar";
  private static final String CAP_ARGON2ID_MAXMEM = "argon2id.maxmem";
  private static final String CAP_ARGON2ID_MAXSIZE = "argon2id.maxsize";

  private static final int MAXITER_DEFAULT = 3;
  private static final int MAXPAR_DEFAULT = 1;
  private static final int MAXMEM_DEFAULT = 32;
  private static final int MAXSIZE_DEFAULT = 32;

  public ARGON2ID(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter map.");
    }

    Map<Object, Object> map = (Map<Object,Object>) top;

    Argon2Parameters.Builder builder = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
        .withVersion(Argon2Parameters.ARGON2_VERSION_13);

    if (map.get(KEY_ITERATIONS) instanceof Long) {
      int iterations = ((Long) map.get(KEY_ITERATIONS)).intValue();

      if (iterations > MAXITER_DEFAULT) {
        if (null == Capabilities.get(stack, CAP_ARGON2ID_MAXITER)) {
          throw new WarpScriptException(getName() + " missing capabbility '" + CAP_ARGON2ID_MAXITER + "'.");
        }
        if (iterations > Long.parseLong(Capabilities.get(stack, CAP_ARGON2ID_MAXITER))) {
          throw new WarpScriptException(getName() + " number of iterations exceeds capability value.");
        }
      }
      builder = builder.withIterations(iterations);
    }

    if (map.get(KEY_MEMORY) instanceof Long) {
      int memory = ((Long) map.get(KEY_MEMORY)).intValue();
      if (memory > MAXMEM_DEFAULT) {
        if (null == Capabilities.get(stack, CAP_ARGON2ID_MAXMEM)) {
          throw new WarpScriptException(getName() + " missing capabbility '" + CAP_ARGON2ID_MAXMEM + "'.");
        }
        if (memory > Long.parseLong(Capabilities.get(stack, CAP_ARGON2ID_MAXMEM))) {
          throw new WarpScriptException(getName() + " memory exceeds capability value.");
        }
      }
      builder = builder.withMemoryAsKB(memory);
    }

    if (map.get(KEY_PARALLELISM) instanceof Long) {
      int lanes = ((Long) map.get(KEY_PARALLELISM)).intValue();
      if (lanes > MAXPAR_DEFAULT) {
        if (null == Capabilities.get(stack, CAP_ARGON2ID_MAXPAR)) {
          throw new WarpScriptException(getName() + " missing capabbility '" + CAP_ARGON2ID_MAXPAR + "'.");
        }
        if (lanes > Long.parseLong(Capabilities.get(stack, CAP_ARGON2ID_MAXPAR))) {
          throw new WarpScriptException(getName() + " lane count exceeds capability value.");
        }
      }
      builder = builder.withParallelism(lanes);
    }

    if (map.get(KEY_SALT) instanceof byte[]) {
      builder = builder.withSalt((byte[]) map.get(KEY_SALT));
    }

    if (map.get(KEY_SECRET) instanceof byte[]) {
      builder = builder.withSecret((byte[]) map.get(KEY_SECRET));
    }

    if (map.get(KEY_ADDITIONAL) instanceof byte[]) {
      builder = builder.withAdditional((byte[]) map.get(KEY_ADDITIONAL));
    }

    int size = MAXSIZE_DEFAULT;

    if (map.get(KEY_SIZE) instanceof Long) {
      size = ((Long) map.get(KEY_SIZE)).intValue();
      if (size > MAXSIZE_DEFAULT) {
        if (null == Capabilities.get(stack, CAP_ARGON2ID_MAXSIZE)) {
          throw new WarpScriptException(getName() + " missing capabbility '" + CAP_ARGON2ID_MAXSIZE + "'.");
        }
        if (size > Long.parseLong(Capabilities.get(stack, CAP_ARGON2ID_MAXSIZE))) {
          throw new WarpScriptException(getName() + " requested output size exceeds capability value.");
        }
      }
    }

    byte[] password = null;

    if (map.get(KEY_PASSWORD) instanceof byte[]) {
      password = (byte[]) map.get(KEY_PASSWORD);
    } else if (map.get(KEY_PASSWORD) instanceof String) {
      password = ((String) map.get(KEY_PASSWORD)).getBytes(StandardCharsets.UTF_8);
    } else {
      throw new WarpScriptException(getName() + " missing password as either STRING or BYTES.");
    }

    byte[] out = new byte[size]; // min 4, max via capability

    Argon2BytesGenerator generator = new Argon2BytesGenerator();
    generator.init(builder.build());

    generator.generateBytes(password, out);
    stack.push(out);
    return stack;
  }
}
