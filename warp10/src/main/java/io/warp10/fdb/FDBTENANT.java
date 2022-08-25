package io.warp10.fdb;

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

    if (null == Capabilities.get(stack, FDBUtils.CAPABILITY_ADMIN)) {
      throw new WarpScriptException(getName() + " missing capability.");
    }

    StoreClient sc = stack.getStoreClient();

    if (!(sc instanceof FDBStoreClient)) {
      throw new WarpScriptException(getName() + " invalid store, not using FoundationDB!");
    }

    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expected a tenant name.");
    }

    FDBStoreClient fsc = (FDBStoreClient) sc;

    FDBPool pool = fsc.getPool();

    FDBContext context = pool.getContext();

    stack.push(FDBUtils.getTenantInfo(context, (String) top));

    return stack;
  }
}
