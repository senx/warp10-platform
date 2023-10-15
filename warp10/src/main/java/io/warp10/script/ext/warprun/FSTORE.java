//
//   Copyright 2022-2023  SenX S.A.S.
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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class FSTORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FSTORE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    boolean overwrite = false;

    if (top instanceof Boolean) {
      overwrite = Boolean.TRUE.equals(top);
      top = stack.pop();
    }

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a target file path.");
    }

    Path path = new Path((String) top);

    top = stack.pop();

    byte[] data = null;

    if (top instanceof byte[]) {
      data = (byte[]) top;
    } else if (top instanceof String) {
      data = ((String) top).getBytes(StandardCharsets.UTF_8);
    } else {
      throw new WarpScriptException(getName() + " operates on a STRING or BYTES.");
    }

    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    OutputStream out = null;

    try {
      FileSystem fs = path.getFileSystem(conf);
      fs.setWriteChecksum(false);
      out = fs.create(path, overwrite);
      out.write(data);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error writing '" + path + "'.", ioe);
    } finally {
      if (null != out) {
        try { out.close(); } catch (Exception e) { throw new WarpScriptException(getName() + " error closing '" + path + "'.", e); }
      }
    }

    return stack;
  }
}
