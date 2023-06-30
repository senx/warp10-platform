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

package io.warp10.fdb;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.warp10.continuum.store.StoreClient;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class FDBTENANT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FDBTENANT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    String regexp;

    if (null != Capabilities.get(stack, FDBUtils.CAPABILITY_ADMIN)) {
      regexp = null;
    } else if (null != Capabilities.get(stack, FDBUtils.CAPABILITY_TENANT)) {
      regexp = Capabilities.get(stack, FDBUtils.CAPABILITY_TENANT);
      // Empty string means match all tenants
      if ("".equals(regexp)) {
        regexp = null;
      }
    } else {
      throw new WarpScriptException(getName() + " missing '" + FDBUtils.CAPABILITY_TENANT + "' or '" + FDBUtils.CAPABILITY_ADMIN + "' capability.");
    }

    StoreClient sc = stack.getStoreClient();

    if (!(sc instanceof FDBStoreClient)) {
      throw new WarpScriptException(getName() + " invalid store, not using FoundationDB!");
    }

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expected a tenant name.");
    }

    String tenant = (String) top;

    if (null != regexp) {
      Matcher m = Pattern.compile(regexp).matcher(tenant);

      if (!m.matches()) {
        throw new WarpScriptException(getName() + " capability '" + FDBUtils.CAPABILITY_TENANT + "' does not allow you to access this tenant.");
      }
    }

    FDBStoreClient fsc = (FDBStoreClient) sc;

    FDBPool pool = fsc.getPool();

    FDBContext context = pool.getContext();

    stack.push(FDBUtils.getTenantInfo(context, tenant));

    return stack;
  }
}
