//
//   Copyright 2025  SenX S.A.S.
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

package io.warp10.script.functions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.KVStoreRequest;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class KVSTORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * KV are stored with keys starting with 'X' (as Xtra data)
   */
  public static final byte[] KVPREFIX = "X".getBytes(StandardCharsets.UTF_8);

  /**
   * Token attribute for storing OPB64 encoded key prefix
   */
  public static final String ATTR_KVPREFIX = ".kvprefix";

  /**
   * Maximum key size
   */
  public static final String ATTR_KVMAXK = ".kvmaxk";

  /**
   * Maximum value size
   */
  public static final String ATTR_KVMAXV = ".kvmaxv";

  private static final int DEFAULT_MAXK;
  private static final int DEFAULT_MAXV;

  static {
    DEFAULT_MAXK = Integer.parseInt(WarpConfig.getProperty(Configuration.WARP_KVSTORE_MAXK, "128"));
    DEFAULT_MAXV = Integer.parseInt(WarpConfig.getProperty(Configuration.WARP_KVSTORE_MAXV, "1024"));
  }

  public KVSTORE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " missing token.");
    }

    //
    // If the token has an attribute .kvprefix then the call can succeed. This attribute
    // acts as a capability.
    //

    WriteToken wtoken = Tokens.extractWriteToken((String) top);

    top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " operates on a map.");
    }

    Map<Object,Object> kv = (Map<Object,Object>) top;

    KVStoreRequest kvsr = new KVStoreRequest();
    kvsr.setToken(wtoken);

    int kvmaxv = DEFAULT_MAXV;
    int kvmaxk = DEFAULT_MAXK;

    if (wtoken.getAttributesSize() > 0) {
      if (null != wtoken.getAttributes().get(ATTR_KVMAXK)) {
        kvmaxk = Integer.parseInt(wtoken.getAttributes().get(ATTR_KVMAXK));
      }
      if (null != wtoken.getAttributes().get(ATTR_KVMAXV)) {
        kvmaxv = Integer.parseInt(wtoken.getAttributes().get(ATTR_KVMAXV));
      }
    }

    for (Entry<Object,Object> entry: kv.entrySet()) {
      byte[] key = null;
      byte[] value = null;

      if (entry.getKey() instanceof byte[]) {
        key = (byte[]) entry.getKey();
      } else if (entry.getKey() instanceof String) {
        key = ((String) entry.getKey()).getBytes(StandardCharsets.UTF_8);
        // Check that STRING keys do not contain Unicode characters
        if (key.length != ((String) entry.getKey()).length()) {
          throw new WarpScriptException(getName() + " STRING keys cannot contain non ISO-8859-1 characters.");
        }
      } else {
        throw new WarpScriptException(getName() + " keys are expected to be BYTES or STRING.");
      }

      if (entry.getValue() instanceof byte[]) {
        value = (byte[]) entry.getValue();
      } else if (entry.getValue() instanceof String) {
        value = ((String) entry.getValue()).getBytes(StandardCharsets.UTF_8);
      } else if (null != entry.getValue()) {
        throw new WarpScriptException(getName() + " values are expected to be BYTES or STRING.");
      }

      if (key.length > kvmaxk) {
        throw new WarpScriptException(getName() + " maximum allowed key size is " + kvmaxk + " bytes.");
      }

      if (null != value && value.length > kvmaxv) {
        throw new WarpScriptException(getName() + " maximum allowed value size is " + kvmaxv + " bytes.");
      }

      kvsr.addToKeys(ByteBuffer.wrap(key));

      if (null == value) {
        kvsr.addToValues(ByteBuffer.allocate(0));
      } else {
        kvsr.addToValues(ByteBuffer.wrap(value));
      }
    }

    StoreClient store = stack.getStoreClient();

    try {
      store.kvstore(kvsr);
      // We force writing a null to trigger a possible sync of a DatalogManager
      store.kvstore(null);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " encountered an error while storing Key/Value pairs.", ioe);
    }

    return stack;
  }
}
