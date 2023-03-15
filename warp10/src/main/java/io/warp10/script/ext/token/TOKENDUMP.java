//
//   Copyright 2019-2023 SenX S.A.S.
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

import io.warp10.continuum.Tokens;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.encoder.QuasarTokenDecoder;
import io.warp10.quasar.encoder.QuasarTokenEncoder;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;

import org.apache.thrift.TBase;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a token and generates a structure for TOKENGEN to recreate an identical token
 */
public class TOKENDUMP extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final QuasarTokenEncoder encoder = new QuasarTokenEncoder();

  public static final String KEY_PARAMS = "params";

  private final byte[] keystoreTokenAESKey;
  private final byte[] keystoreTokenSipHashKey;

  /**
   * Whether the keystore used to initialize the keys is that of a Warp/WarpDist instance.
   * If true, a secret is needed to access the keystore keys.
   */
  private final boolean warpKeystore;

  /**
   * Create the TOKENDUMP function.
   * @param name The name of the function.
   * @param keystore The keystore containing the AES and SipHash keys to decode tokens when no such keys are given when applying this function.
   * @param warpKeystore Whether the given keystore is that of a Warp/WarpDist instance. If true, a secret is needed to access the keystore keys.
   */
  public TOKENDUMP(String name, KeyStore keystore, boolean warpKeystore) {
    super(name);
    if(null != keystore) {
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
    boolean customKeys = false;

    // First, check if SipHash and AES keys are explicitly defined. In that case, no need for secret.
    Object top = stack.pop();
    if (top instanceof byte[]) {
      customKeys = true;

      tokenSipHashKey = (byte[]) top;

      top = stack.pop();

      if (!(top instanceof byte[])) {
        throw new WarpScriptException(getName() + " expects a BYTES AES Key if a BYTES SipHash is given.");
      }

      tokenAESKey = (byte[]) top;

      top = stack.pop();
    }

    if (null == tokenAESKey) { // in that case we have also null == tokenSipHashKey
      // SipHash and AES keys are not explicitly defined, so we fall back to those of the keystore.
      // Check the secret if needed before the fallback.
      if (warpKeystore) {
        if (null == Capabilities.get(stack, TokenWarpScriptExtension.CAPABILITY_TOKENDUMP)) {
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

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a STRING token.");
    }

    String tokenstr = (String) top;

    ReadToken rtoken = null;
    WriteToken wtoken = null;

    if (!customKeys && warpKeystore) {
      // In that case, we don't even need tokenAESKey and tokenSipHashKey and use directly the Tokens class.
      // It has the advantage of decoding tokens defined in files.
      try {
        rtoken = Tokens.extractReadToken(tokenstr);
      } catch (WarpScriptException wse) {
        try {
          wtoken = Tokens.extractWriteToken(tokenstr);
        } catch (Exception e) {
          throw new WarpScriptException(getName() + " invalid token.", e);
        }
      }
    } else {
      byte[] token = OrderPreservingBase64.decode(tokenstr.getBytes(StandardCharsets.UTF_8));

      long[] lkey = SipHashInline.getKey(tokenSipHashKey);
      QuasarTokenDecoder dec = new QuasarTokenDecoder(lkey[0], lkey[1], tokenAESKey);

      try {
        rtoken = dec.decodeReadToken(token);
      } catch (QuasarTokenException qte) {
        try {
          wtoken = dec.decodeWriteToken(token);
        } catch (Exception e) {
          throw new WarpScriptException(getName() + " invalid token.", e);
        }
      }
    }

    String ident = encoder.getTokenIdent(tokenstr, tokenSipHashKey);

    Map<Object, Object> result = new HashMap<Object, Object>();
    result.put(TOKENGEN.KEY_TOKEN, tokenstr);
    result.put(TOKENGEN.KEY_IDENT, ident);
    result.put(KEY_PARAMS, mapFromToken(null != rtoken ? rtoken : wtoken));

    stack.push(result);

    return stack;
  }

  public static Map<String, Object> mapFromToken(TBase token) {
    Map<String, Object> params = new HashMap<String, Object>();

    if (token instanceof ReadToken) {
      ReadToken rtoken = (ReadToken) token;
      params.put(TOKENGEN.KEY_TYPE, TokenType.READ.toString());
      params.put(TOKENGEN.KEY_OWNER, Tokens.getUUID(rtoken.getBilledId()));
      params.put(TOKENGEN.KEY_APPLICATION, rtoken.getAppName());
      params.put(TOKENGEN.KEY_ISSUANCE, rtoken.getIssuanceTimestamp());
      params.put(TOKENGEN.KEY_EXPIRY, rtoken.getExpiryTimestamp());


      if (rtoken.getOwnersSize() > 0) {
        List<String> owners = new ArrayList<String>(rtoken.getOwnersSize());
        params.put(TOKENGEN.KEY_OWNERS, owners);
        for (ByteBuffer bb: rtoken.getOwners()) {
          owners.add(Tokens.getUUID(bb));
        }
      }

      if (rtoken.getProducersSize() > 0) {
        List<String> producers = new ArrayList<String>(rtoken.getProducersSize());
        params.put(TOKENGEN.KEY_PRODUCERS, producers);
        for (ByteBuffer bb: rtoken.getProducers()) {
          producers.add(Tokens.getUUID(bb));
        }
      }

      if (rtoken.getAppsSize() > 0) {
        List<String> applications = new ArrayList<String>(rtoken.getAppsSize());
        params.put(TOKENGEN.KEY_APPLICATIONS, applications);
        for (String app: rtoken.getApps()) {
          applications.add(app);
        }
      }

      if (rtoken.getAttributesSize() > 0) {
        Map<String, String> attr = new HashMap<String, String>(rtoken.getAttributes());
        params.put(TOKENGEN.KEY_ATTRIBUTES, attr);
      }

      if (rtoken.getLabelsSize() > 0) {
        Map<String, String> labels = new HashMap<String, String>(rtoken.getLabels());
        params.put(TOKENGEN.KEY_LABELS, labels);
      }
    } else {
      WriteToken wtoken = (WriteToken) token;

      params.put(TOKENGEN.KEY_TYPE, TokenType.WRITE.toString());
      params.put(TOKENGEN.KEY_OWNER, Tokens.getUUID(wtoken.getOwnerId()));
      params.put(TOKENGEN.KEY_PRODUCER, Tokens.getUUID(wtoken.getProducerId()));
      params.put(TOKENGEN.KEY_APPLICATION, wtoken.getAppName());
      params.put(TOKENGEN.KEY_ISSUANCE, wtoken.getIssuanceTimestamp());
      params.put(TOKENGEN.KEY_EXPIRY, wtoken.getExpiryTimestamp());

      if (wtoken.getAttributesSize() > 0) {
        Map<String, String> attr = new HashMap<String, String>(wtoken.getAttributes());
        params.put(TOKENGEN.KEY_ATTRIBUTES, attr);
      }

      if (wtoken.getLabelsSize() > 0) {
        Map<String, String> labels = new HashMap<String, String>(wtoken.getLabels());
        params.put(TOKENGEN.KEY_LABELS, labels);
      }
    }
    return params;
  }
}
