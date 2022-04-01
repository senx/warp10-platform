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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.FOREACH;
import io.warp10.script.WarpScriptStack.Macro;

public class FREAD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final FOREACH FOREACH = new FOREACH(WarpScriptLib.FOREACH);

  public FREAD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a file path.");
    }

    Path path = new Path((String) top);
    top = stack.pop();

    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a MACRO.");
    }

    Macro macro = (Macro) top;

    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    InputStream in = null;

    try {
      FileSystem fs = path.getFileSystem(conf);
      in = fs.open(path);
      Iterator<String> iter = new STDIN.InputStreamIterator(in);

      stack.push(iter);
      stack.push(macro);
      stack.push(false);
      FOREACH.apply(stack);
    } catch (IOException ioe) {
      new WarpScriptException(getName() + " error processing '" + path + "'.", ioe);
    } finally {
      if (null != in) {
        try { in.close(); } catch (Exception e) { throw new WarpScriptException(getName() + " error closing '" + path + "'.", e); }
      }
    }

    return stack;
  }
}
