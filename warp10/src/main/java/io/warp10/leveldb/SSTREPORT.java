//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10.leveldb;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.impl.Filename;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

/**
 * Build an SST report in the way WarpReport does it.
 */
public class SSTREPORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SSTREPORT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_REPORT)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_REPORT + "'.");
    }

    //
    // Read the CURRENT file to determine the current MANIFEST
    //

    try {
      WarpDB db = Warp.getDB();

      if (null == db) {
        throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
      }

      String leveldbhome = db.getHome();

      //
      // Get report
      //

      File currentFile = new File(leveldbhome, Filename.currentFileName());
      Preconditions.checkState(currentFile.exists(), "CURRENT file does not exist");

      String currentName = Files.toString(currentFile, Charsets.UTF_8);
      if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
        throw new IllegalStateException("CURRENT file does not end with newline");
      }
      currentName = currentName.substring(0, currentName.length() - 1);

      String manifest = currentName;

      Object result = WarpReport.report(new File(leveldbhome, manifest).getAbsolutePath());

      stack.push(result);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " encountered an error while reading LevelDB MANIFEST file.", ioe);
    }
    return stack;
  }
}
