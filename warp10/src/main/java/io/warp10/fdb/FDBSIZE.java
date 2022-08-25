package io.warp10.fdb;

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

    Object top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " 'to' key must be a byte array.");
    }

    byte[] to = (byte[]) top;

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " 'from' key must be a byte array.");
    }

    byte[] from = (byte[]) top;

    FDBStoreClient fsc = (FDBStoreClient) sc;

    FDBPool pool = fsc.getPool();

    FDBContext context = pool.getContext();

    stack.push(FDBUtils.getEstimatedRangeSizeBytes(context, from, to));

    return stack;
  }
}
