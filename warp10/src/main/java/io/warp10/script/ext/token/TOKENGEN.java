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

package io.warp10.script.ext.token;

import io.warp10.crypto.KeyStore;
import io.warp10.quasar.encoder.QuasarTokenEncoder;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TOKENGEN extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final static QuasarTokenEncoder encoder = new QuasarTokenEncoder();

  public static final String KEY_TOKEN = "token";
  public static final String KEY_IDENT = "ident";

  public static final String KEY_ID = "id";

  public static final String KEY_TYPE = "type";
  public static final String KEY_APPLICATION = "application";
  public static final String KEY_EXPIRY = "expiry";
  public static final String KEY_ISSUANCE = "issuance";
  public static final String KEY_TTL = "ttl";
  public static final String KEY_LABELS = "labels";
  public static final String KEY_ATTRIBUTES = "attributes";
  public static final String KEY_OWNERS = "owners";
  public static final String KEY_OWNER = "owner";
  public static final String KEY_PRODUCERS = "producers";
  public static final String KEY_PRODUCER = "producer";
  public static final String KEY_APPLICATIONS = "applications";

  private static final long DEFAULT_TTL = 0;

  private final byte[] keystoreTokenAESKey;
  private final byte[] keystoreTokenSipHashKey;

  /**
   * Whether the keystore used to initialize the keys is that of a Warp/WarpDist instance.
   * If true, a secret is needed to access the keystore keys.
   */
  private final boolean warpKeystore;

  /**
   * Create the TOKENGEN function.
   * @param name The name of the function.
   * @param keystore The keystore containing the AES and SipHash keys to decode tokens when no such keys are given when applying this function.
   * @param warpKeystore Whether the given keystore is that of a Warp/WarpDist instance. If true, a secret is needed to access the keystore keys.
   */
  public TOKENGEN(String name, KeyStore keystore, boolean warpKeystore) {
    super(name);
    if (null != keystore) {
      keystoreTokenAESKey = keystore.getKey(KeyStore.AES_TOKEN);
      keystoreTokenSipHashKey = keystore.getKey(KeyStore.SIPHASH_TOKEN);
    } else {
      keystoreTokenAESKey = null;
      keystoreTokenSipHashKey = null;
    }
    this.warpKeystore = warpKeystore;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    byte[] tokenAESKey = null;
    byte[] tokenSipHashKey = null;

    // First, check if SipHash and AES keys are explicitly defined. In that case, no need for secret.
    Object top = stack.pop();

    if (top instanceof byte[]) {
      tokenSipHashKey = (byte[]) top;

      top = stack.pop();

      if (!(top instanceof byte[])) {
        throw new WarpScriptException(getName() + " expects a BYTES AES Key if a BYTES SipHash key is provided.");
      }

      tokenAESKey = (byte[]) top;

      top = stack.pop();
    }

    if (null == tokenAESKey) { // in that case we have also null == tokenSipHashKey
      // SipHash and AES keys are not explicitly defined, so we fall back to those of the keystore.
      // Check the capability before the fallback.
      if (warpKeystore) {
        if (null == Capabilities.get(stack, TokenWarpScriptExtension.CAPABILITY_TOKENGEN)) {
          throw new WarpScriptException(getName() + " missing capability.");
        }
      }

      // Fallback to keystore keys.
      if (null == keystoreTokenAESKey || null == keystoreTokenSipHashKey) {
        throw new WarpScriptException(getName() + " expects SipHash and AES keys to be explicitly defined.");
      } else {
        tokenAESKey = keystoreTokenAESKey;
        tokenSipHashKey = keystoreTokenSipHashKey;
      }
    }

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map on top of the stack.");
    }

    Map<Object, Object> params = (Map<Object, Object>) top;

    try {
      TBase token = tokenFromMap(params, getName(), DEFAULT_TTL);

      String tokenstr = encoder.encryptToken(token, tokenAESKey, tokenSipHashKey);
      String ident = encoder.getTokenIdent(tokenstr, tokenSipHashKey);

      Map<Object, Object> result = new HashMap<Object, Object>();
      result.put(KEY_TOKEN, tokenstr);
      result.put(KEY_IDENT, ident);
      if (null != params.get(KEY_ID)) {
        result.put(KEY_ID, params.get(KEY_ID));
      }
      stack.push(result);
    } catch (TException te) {
      throw new WarpScriptException("Error while generating token.", te);
    }

    return stack;
  }

  public static TBase tokenFromMap(Map params, String name, long defaultTtl) throws WarpScriptException {
    TBase token = null;

    if (TokenType.READ.toString().equals(params.get(KEY_TYPE))) {
      ReadToken rtoken = new ReadToken();
      rtoken.setTokenType(TokenType.READ);

      if (null != params.get(KEY_OWNER)) {
        rtoken.setBilledId(encoder.toByteBuffer(params.get(KEY_OWNER).toString()));
      } else {
        throw new WarpScriptException(name + " missing '" + KEY_OWNER + "'.");
      }

      if (null == params.get(KEY_APPLICATION)) {
        throw new WarpScriptException(name + " missing '" + KEY_APPLICATION + "'.");
      }
      rtoken.setAppName(params.get(KEY_APPLICATION).toString());

      if (null != params.get(KEY_ISSUANCE)) {
        rtoken.setIssuanceTimestamp(((Number) params.get(KEY_ISSUANCE)).longValue());
      } else {
        rtoken.setIssuanceTimestamp(System.currentTimeMillis());
      }

      if (null != params.get(KEY_TTL)) {
        long ttl = ((Number) params.get(KEY_TTL)).longValue();
        try {
          rtoken.setExpiryTimestamp(Math.addExact(ttl, rtoken.getIssuanceTimestamp()));
        } catch (ArithmeticException ae) {
          rtoken.setExpiryTimestamp(Long.MAX_VALUE);
        }
      } else if (null != params.get(KEY_EXPIRY)) {
        rtoken.setExpiryTimestamp(((Number) params.get(KEY_EXPIRY)).longValue());
      } else if (0L == defaultTtl) {
        throw new WarpScriptException(name + " missing '" + KEY_TTL + "' or '" + KEY_EXPIRY + "'.");
      } else {
        try {
          rtoken.setExpiryTimestamp(Math.addExact(defaultTtl, rtoken.getIssuanceTimestamp()));
        } catch (ArithmeticException ae) {
          rtoken.setExpiryTimestamp(Long.MAX_VALUE);
        }
      }

      if (null != params.get(KEY_OWNERS)) {
        if (!(params.get(KEY_OWNERS) instanceof List)) {
          throw new WarpScriptException(name + " expects '" + KEY_OWNERS + "' to be a list of UUIDs.");
        }
        for (Object uuid: (List) params.get(KEY_OWNERS)) {
          rtoken.addToOwners(encoder.toByteBuffer(uuid.toString()));
        }
        if (0 == rtoken.getOwnersSize()) {
          rtoken.setOwners(new ArrayList<ByteBuffer>(0));
        }
      } else {
        rtoken.setOwners(new ArrayList<ByteBuffer>(0));
      }

      if (null != params.get(KEY_PRODUCERS)) {
        if (!(params.get(KEY_PRODUCERS) instanceof List)) {
          throw new WarpScriptException(name + " expects '" + KEY_PRODUCERS + "' to be a list of UUIDs.");
        }
        for (Object uuid: (List) params.get(KEY_PRODUCERS)) {
          rtoken.addToProducers(encoder.toByteBuffer(uuid.toString()));
        }
        if (0 == rtoken.getProducersSize()) {
          rtoken.setProducers(new ArrayList<ByteBuffer>(0));
        }
      } else {
        rtoken.setProducers(new ArrayList<ByteBuffer>(0));
      }

      if (null != params.get(KEY_APPLICATIONS)) {
        if (!(params.get(KEY_APPLICATIONS) instanceof List)) {
          throw new WarpScriptException(name + " expects '" + KEY_APPLICATIONS + "' to be a list of application names.");
        }
        for (Object app: (List) params.get(KEY_APPLICATIONS)) {
          rtoken.addToApps(app.toString());
        }
        if (0 == rtoken.getAppsSize()) {
          rtoken.setApps(new ArrayList<String>(0));
        }
      } else {
        rtoken.setApps(new ArrayList<String>(0));
      }

      if (null != params.get(KEY_ATTRIBUTES)) {
        if (!(params.get(KEY_ATTRIBUTES) instanceof Map)) {
          throw new WarpScriptException(name + " expects '" + KEY_ATTRIBUTES + "' to be a map.");
        }
        for (Entry<Object, Object> entry: ((Map<Object, Object>) params.get(KEY_ATTRIBUTES)).entrySet()) {
          if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
            throw new WarpScriptException(name + " expects '" + KEY_ATTRIBUTES + "' to be a map of STRING keys and values.");
          }
          rtoken.putToAttributes(entry.getKey().toString(), entry.getValue().toString());
        }
      }

      if (null != params.get(KEY_LABELS)) {
        if (!(params.get(KEY_LABELS) instanceof Map)) {
          throw new WarpScriptException(name + " expects '" + KEY_LABELS + "' to be a map.");
        }
        for (Entry<Object, Object> entry: ((Map<Object, Object>) params.get(KEY_LABELS)).entrySet()) {
          if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
            throw new WarpScriptException(name + " expects '" + KEY_LABELS + "' to be a map of STRING keys and values.");
          }
          rtoken.putToLabels(entry.getKey().toString(), entry.getValue().toString());
        }
      }

      token = rtoken;
    } else if (TokenType.WRITE.toString().equals(params.get(KEY_TYPE))) {
      WriteToken wtoken = new WriteToken();
      wtoken.setTokenType(TokenType.WRITE);

      if (null == params.get(KEY_APPLICATION)) {
        throw new WarpScriptException(name + " missing '" + KEY_APPLICATION + "'.");
      }
      wtoken.setAppName(params.get(KEY_APPLICATION).toString());

      if (null != params.get(KEY_ISSUANCE)) {
        wtoken.setIssuanceTimestamp(((Number) params.get(KEY_ISSUANCE)).longValue());
      } else {
        wtoken.setIssuanceTimestamp(System.currentTimeMillis());
      }

      if (null != params.get(KEY_TTL)) {
        long ttl = ((Number) params.get(KEY_TTL)).longValue();
        try {
          wtoken.setExpiryTimestamp(Math.addExact(ttl, wtoken.getIssuanceTimestamp()));
        } catch (ArithmeticException ae) {
          wtoken.setExpiryTimestamp(Long.MAX_VALUE);
        }
      } else if (null != params.get(KEY_EXPIRY)) {
        wtoken.setExpiryTimestamp(((Number) params.get(KEY_EXPIRY)).longValue());
      } else if (0L == defaultTtl) {
        throw new WarpScriptException(name + " missing '" + KEY_TTL + "' or '" + KEY_EXPIRY + "'.");
      } else {
        try {
          wtoken.setExpiryTimestamp(Math.addExact(defaultTtl, wtoken.getIssuanceTimestamp()));
        } catch (ArithmeticException ae) {
          wtoken.setExpiryTimestamp(Long.MAX_VALUE);
        }
      }

      if (null != params.get(KEY_OWNER)) {
        wtoken.setOwnerId(encoder.toByteBuffer(params.get(KEY_OWNER).toString()));
      } else {
        throw new WarpScriptException(name + " missing '" + KEY_OWNER + "'.");
      }

      if (null != params.get(KEY_PRODUCER)) {
        wtoken.setProducerId(encoder.toByteBuffer(params.get(KEY_PRODUCER).toString()));
      } else {
        throw new WarpScriptException(name + " missing '" + KEY_PRODUCER + "'.");
      }

      if (null != params.get(KEY_ATTRIBUTES)) {
        if (!(params.get(KEY_ATTRIBUTES) instanceof Map)) {
          throw new WarpScriptException(name + " expects '" + KEY_ATTRIBUTES + "' to be a map.");
        }
        for (Entry<Object, Object> entry: ((Map<Object, Object>) params.get(KEY_ATTRIBUTES)).entrySet()) {
          if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
            throw new WarpScriptException(name + " expects '" + KEY_ATTRIBUTES + "' to be a map of STRING keys and values.");
          }
          wtoken.putToAttributes(entry.getKey().toString(), entry.getValue().toString());
        }
      }

      if (null != params.get(KEY_LABELS)) {
        if (!(params.get(KEY_LABELS) instanceof Map)) {
          throw new WarpScriptException(name + " expects '" + KEY_LABELS + "' to be a map.");
        }
        for (Entry<Object, Object> entry: ((Map<Object, Object>) params.get(KEY_LABELS)).entrySet()) {
          if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
            throw new WarpScriptException(name + " expects '" + KEY_LABELS + "' to be a map of STRING keys and values.");
          }
          wtoken.putToLabels(entry.getKey().toString(), entry.getValue().toString());
        }
      }

      token = wtoken;
    } else {
      throw new WarpScriptException(name + " expects a key '" + KEY_TYPE + "' with value READ or WRITE in the parameter map.");
    }

    return token;
  }
}
