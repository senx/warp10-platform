//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10.continuum;

import java.util.Map;

import org.apache.thrift.TBase;

import io.warp10.WarpConfig;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.filter.QuasarTokenFilter;
import io.warp10.quasar.filter.exception.QuasarApplicationRevoked;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.filter.exception.QuasarTokenExpired;
import io.warp10.quasar.filter.exception.QuasarTokenRevoked;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.quasar.trl.QuasarTokenRevocationListLoader;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpFleetMacroRepository;
import io.warp10.script.WarpScriptException;
import io.warp10.script.ext.token.TOKENDUMP;
import io.warp10.script.ext.token.TOKENGEN;

public class MacroFilterAuthenticationPlugin implements AuthenticationPlugin {

  private static final String MACRO;
  private static final QuasarTokenFilter FILTER;

  static {
    FILTER = Tokens.getTokenFilter();
    MACRO = WarpConfig.getProperty(Configuration.WARP_TOKEN_FILTER_MACRO);
  }

  @Override
  public ReadToken extractReadToken(String token) throws WarpScriptException {
    if (null == MACRO || null == token || "".equals(token)) {
      return null;
    }

    return (ReadToken) extractToken(token, true);
  }

  @Override
  public WriteToken extractWriteToken(String token) throws WarpScriptException {
    if (null == MACRO) {
      return null;
    }

    return (WriteToken) extractToken(token, true);
  }

  private static TBase extractToken(String token, boolean read) throws WarpScriptException {
    byte[] tokenB64Data = token.getBytes();

    // check if the token is revoked by the owner
    boolean revoked = false;
    try {
      FILTER.getTokensRevoked().isTokenRevoked(FILTER.getTokenSipHash(tokenB64Data));
    } catch (QuasarTokenRevoked qtr) {
      revoked = true;
    } catch (QuasarTokenException qte) {
    }

    // Decode the token hex string to byte array
    byte[] tokenHexData = OrderPreservingBase64.decode(tokenB64Data);

    TBase rwtoken = null;
    String appName = null;
    long expiry = Long.MIN_VALUE;
    long issuance = Long.MIN_VALUE;

    try {
      if (read) {
        ReadToken rtoken = FILTER.getTokenDecoder().decodeReadToken(tokenHexData);
        appName = rtoken.getAppName();
        expiry = rtoken.getExpiryTimestamp();
        issuance = rtoken.getIssuanceTimestamp();
        rwtoken = rtoken;
      } else {
        WriteToken wtoken = FILTER.getTokenDecoder().decodeWriteToken(tokenHexData);
        appName = wtoken.getAppName();
        expiry = wtoken.getExpiryTimestamp();
        issuance = wtoken.getIssuanceTimestamp();
        rwtoken = wtoken;
      }
    } catch (QuasarTokenException qte) {
      throw new WarpScriptException("Invalid token", qte);
    }

    // compute the app id
    long appId = QuasarTokenRevocationListLoader.getApplicationHash(appName);

    // check the token expiration
    boolean expired = false;
    try {
      FILTER.checkTokenExpired(issuance, expiry, appId);
    } catch (QuasarTokenExpired qte) {
      expired = true;
    }

    // check the registered application status
    boolean appRevoked = false;

    try {
      FILTER.getTokensRevoked().isRegisteredAppAuthorized(appId);
    } catch (QuasarApplicationRevoked qar) {
      appRevoked = true;
    } catch (QuasarTokenException qte) {
    }

    Map<String,Object> map = TOKENDUMP.mapFromToken(rwtoken);

    map.put("token.expired", expired);
    map.put("token.revoked", revoked);
    map.put("app.revoked", appRevoked);

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();
    stack.setAttribute(WarpFleetMacroRepository.ATTRIBUTE_WARPFLEET_DISABLE, true);
    stack.push(map);
    stack.run(MACRO);
    map = (Map<String,Object>) stack.pop();
    return TOKENGEN.tokenFromMap(map, "", Long.MAX_VALUE);
  }
}
