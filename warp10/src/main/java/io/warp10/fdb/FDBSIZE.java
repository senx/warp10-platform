package io.warp10.fdb;

import java.util.Map;

import io.warp10.continuum.store.StoreClient;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

public class FDBSIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public FDBSIZE(String name) {
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

    FDBStoreClient fsc = (FDBStoreClient) sc;
    FDBPool pool = fsc.getPool();

    Object top = stack.pop();

    byte[] from = null;
    byte[] to = null;

    if (top instanceof String) {
      Map<String,Object> tenant = FDBUtils.getTenantInfo(pool.getDatabase(), (String) top);

      if (tenant.containsKey(FDBUtils.KEY_PREFIX)) {
        from = (byte[]) tenant.get(FDBUtils.KEY_PREFIX);
        to = FDBUtils.getNextKey(from);
      } else {
        throw new WarpScriptException(getName() + " unknown tenant.");
      }
    } else {
      if (!(top instanceof byte[])) {
        throw new WarpScriptException(getName() + " 'to' key must be a byte array.");
      }

      to = (byte[]) top;

      top = stack.pop();

      if (!(top instanceof byte[])) {
        throw new WarpScriptException(getName() + " 'from' key must be a byte array.");
      }

      from = (byte[]) top;
    }

    FDBContext context = pool.getContext();

    stack.push(FDBUtils.getEstimatedRangeSizeBytes(context, from, to));

    return stack;
  }
}
