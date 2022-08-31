//
//   Copyright 2022  SenX S.A.S.
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

package io.warp10.script.ext.warprun;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class STDIN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  static final class InputStreamIterator implements Iterator<String> {

    private final BufferedReader br;

    public InputStreamIterator(InputStream in) {
      this.br = new BufferedReader(new InputStreamReader(in));
    }

    private String line = null;
    private boolean done = false;

    @Override
    public boolean hasNext() {
      if (done) {
        return false;
      }
      if (null == line) {
        try {
          line = br.readLine();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
      done = null == line;
      return !done;
    }

    @Override
    public String next() {
      if (null == line) {
        throw new IllegalStateException();
      } else {
        String result = line;
        line = null;
        return result;
      }
    }
  };

  private static final Iterator<String> stdin = new InputStreamIterator(System.in);

  public STDIN(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    stack.push(stdin);
    return stack;
  }
}
