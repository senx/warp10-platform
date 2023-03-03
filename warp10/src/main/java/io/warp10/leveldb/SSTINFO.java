//
//   Copyright 2019-2023  SenX S.A.S.
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
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import org.iq80.leveldb.impl.Filename;

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

/**
 * Returns informations about an SST file
 */
public class SSTINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String SIZE_KEY = "size";
  private static final String CREATIONTIME_KEY = "creationTime";

  public SSTINFO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_INFO)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_INFO + "'.");
    }

    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects an SST file id.");
    }

    long number = (Long) top;

    WarpDB db = Warp.getDB();

    if (null == db) {
      throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
    }

    String leveldbhome = db.getHome();

    try {
      File sstfile = new File(leveldbhome, Filename.tableFileName(number));
      BasicFileAttributes attr = Files.readAttributes(sstfile.toPath(), BasicFileAttributes.class);
      long creationTime = attr.creationTime().toMillis() * Constants.TIME_UNITS_PER_MS;
      long size = attr.size();
      Map<String,Object> infos = new HashMap<String,Object>();
      infos.put(SIZE_KEY, size);
      infos.put(CREATIONTIME_KEY, creationTime);
      stack.push(infos);
    } catch (IOException ioe) {
      throw new WarpScriptException("Error while fetching attributes for SST file " + number, ioe);
    }

    return stack;
  }
}
