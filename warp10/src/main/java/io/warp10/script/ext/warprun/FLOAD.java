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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class FLOAD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FLOAD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a file path.");
    }

    Path path = new Path((String) top);

    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    InputStream in = null;

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try {
      FileSystem fs = path.getFileSystem(conf);
      in = fs.open(path);
      IOUtils.copy(in, out);
      out.close();
      stack.push(out.toByteArray());
    } catch (IOException ioe) {
      new WarpScriptException(getName() + " error loading '" + path + "'.", ioe);
    } finally {
      if (null != in) {
        try { in.close(); } catch (Exception e) { throw new WarpScriptException(getName() + " error closing '" + path + "'.", e); }
      }
    }

    return stack;
  }
}
