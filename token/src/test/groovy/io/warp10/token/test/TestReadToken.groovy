//
//   Copyright 2018  SenX S.A.S.
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
package io.warp10.token.test

import io.warp10.quasar.encoder.QuasarTokenEncoder
import io.warp10.quasar.filter.QuasarTokenFilter
import io.warp10.quasar.filter.exception.QuasarNoToken
import io.warp10.quasar.filter.exception.QuasarTokenExpired
import io.warp10.quasar.filter.exception.QuasarTokenInvalid
import io.warp10.quasar.token.thrift.data.ReadToken
import org.junit.Test

class TestReadToken extends TokenTestCase {

    private QuasarTokenEncoder tokenEncoder = new QuasarTokenEncoder()

    //
    // Standard token read token
    //
    @Test
    void testGenerateAndDecodeStandard() {
        // generate a valid token with
        // owner = producer
        String producer = UUID.randomUUID().toString()
        String app = "warp10.test"
        long ttl = 32468

        String readToken = tokenEncoder.deliverReadToken(app, producer, producer, [app], ttl, getKeyStore())

        // decode it with the token filter
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())
        ReadToken tokenDecoded = tokenFilter.getReadToken(readToken)

        // asserts token values
        assert ttl == ((long) tokenDecoded.expiryTimestamp - (long) tokenDecoded.issuanceTimestamp)

        // this token belongs to this app and producer
        tokenDecoded.appName.equals(app)
        assert Arrays.equals(tokenDecoded.billedId, uuidToBinaryByteArray(producer))

        // matrix' token authorisation
        assert tokenDecoded.apps.size() == 1
        assert tokenDecoded.owners.size() == 1
        assert tokenDecoded.producers.size() == 0
        assert tokenDecoded.apps.contains(app)
        assert tokenDecoded.owners.contains(uuidToByteBuffer(producer))

        // no hooks
        assert tokenDecoded.hooks == null
    }

    //
    // Standard token read token
    //
    @Test
    void testGenerateAndDecodeCustom() {
        // generate a valid token with a exotic authorization matrix
        //
        // this token belongs to
        String tokenOwner = UUID.randomUUID().toString()
        String tokenApp = "warp10.test"

        // this token reads data from 2 other apps
        List<String> apps = ["warp10.app1", "warp10.app2"]
        // from 2 data owners
        List<String> owners = [UUID.randomUUID().toString(), UUID.randomUUID().toString()]

        Map<String, String> hooks = ['PRIVATE_HOOK': 'NOW']

        long ttl = 32468

        String readToken = tokenEncoder.deliverReadToken(tokenApp, tokenOwner, owners, apps, hooks, ttl, getKeyStore())

        // decode it with the token filter
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())
        ReadToken tokenDecoded = tokenFilter.getReadToken(readToken)

        // asserts token values
        assert ttl == ((long) tokenDecoded.expiryTimestamp - (long) tokenDecoded.issuanceTimestamp)

        // this token belongs to this app and producer
        tokenDecoded.appName.equals(tokenApp)
        assert Arrays.equals(tokenDecoded.billedId, uuidToBinaryByteArray(tokenOwner))

        // matrix' token authorisation
        assert tokenDecoded.apps.equals(apps)
        assert tokenDecoded.owners.equals(uuidToByteBuffer(owners))
        assert tokenDecoded.producers.size() == 0

        // hooks
        assert tokenDecoded.hooks.equals(hooks)
    }

    //
    // Expected Errors
    //
    @Test
    void testTokenExpired() {
        // token with a validity of 1ms, its quite short
        String uuid = UUID.randomUUID().toString()
        String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, ["app"], 1, getKeyStore())

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenExpired.class) {
            Thread.sleep(1)
            tokenFilter.getReadToken(readToken)
        }
    }


    @Test
    void testNoToken() {
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarNoToken.class) {
            tokenFilter.getReadToken(null)
        }

        shouldFail(QuasarNoToken.class) {
            tokenFilter.getReadToken("")
        }
    }

    //
    // Token Corruption Errors
    //
    @Test
    void testTokenAESCorrupted() {
        // generate a token
        String uuid = UUID.randomUUID().toString()
        String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, ["app"], 32468, getKeyStore())

        // corrupt the token (pick a random character and decrement it)
        char c1 = readToken.charAt(new Random().nextInt(60))
        char c2 = c1 - 1
        readToken = readToken.replace(c1, c2)

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenInvalid.class) {
            tokenFilter.getReadToken(readToken)
        }
    }

    @Test
    void testTokenB64Corrupted() {
        // generate a token
        String uuid = UUID.randomUUID().toString()
        String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, ["app"], 32468, getKeyStore())

        // corrupt the token (replace a by A)
        readToken = "&~#{[]}".concat(readToken)

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenInvalid.class) {
            tokenFilter.getReadToken(readToken)
        }
    }

    @Test
    void testBadTokenType() {
        String uuid = UUID.randomUUID().toString()
        String writeToken = tokenEncoder.deliverWriteToken("app", uuid, uuid, null, 32468, getKeyStore())

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenInvalid.class) {
            tokenFilter.getReadToken(writeToken)
        }
    }
}
