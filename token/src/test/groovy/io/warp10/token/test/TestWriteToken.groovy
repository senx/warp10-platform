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
import io.warp10.quasar.token.thrift.data.WriteToken
import org.junit.Test

class TestWriteToken extends TokenTestCase {
    private QuasarTokenEncoder tokenEncoder = new QuasarTokenEncoder()

    //
    // Standard token write token
    //
    @Test
    void testGenerateAndDecodeStandard() {
        // generate a valid token with
        // owner = producer
        String uuid = UUID.randomUUID().toString()
        String app = "warp10.test"
        long ttl = 32468

        String writeToken = tokenEncoder.deliverWriteToken(app, uuid, ttl, getKeyStore())

        // decode it with the token filter
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())
        WriteToken tokenDecoded = tokenFilter.getWriteToken(writeToken)

        // asserts token values
        assert ttl == ((long) tokenDecoded.expiryTimestamp - (long) tokenDecoded.issuanceTimestamp)

        // this token belongs to this app and producer
        tokenDecoded.appName.equals(app)
        assert Arrays.equals(tokenDecoded.ownerId, uuidToBinaryByteArray(uuid))
        assert Arrays.equals(tokenDecoded.producerId, uuidToBinaryByteArray(uuid))

        // no fixes labels, no indices
        assert tokenDecoded.labels == null
        assert tokenDecoded.indices == null
    }

    //
    // Standard token write token where
    // owner != producer
    //
    @Test
    void testGenerateAndDecode() {
        // generate a valid token with
        // owner = producer
        String owner = UUID.randomUUID().toString()
        String producer = UUID.randomUUID().toString()
        String app = "warp10.test"
        long ttl = 32468

        String writeToken = tokenEncoder.deliverWriteToken(app, producer, owner, ttl, getKeyStore())

        // decode it with the token filter
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())
        WriteToken tokenDecoded = tokenFilter.getWriteToken(writeToken)

        // asserts token values
        assert ttl == ((long) tokenDecoded.expiryTimestamp - (long) tokenDecoded.issuanceTimestamp)

        // this token belongs to this app and producer
        tokenDecoded.appName.equals(app)
        assert Arrays.equals(tokenDecoded.ownerId, uuidToBinaryByteArray(owner))
        assert Arrays.equals(tokenDecoded.producerId, uuidToBinaryByteArray(producer))

        // no fixes labels, no indices
        assert tokenDecoded.labels == null
        assert tokenDecoded.indices == null
    }

    @Test
    void testGenerateAndDecodeWithLabels() {
        // generate a valid token with
        // owner = producer
        String owner = UUID.randomUUID().toString()
        String producer = UUID.randomUUID().toString()
        String app = "warp10.test"
        Map<String, String> labels = ["labelKey": "labelValue"]
        long ttl = 32468

        String writeToken = tokenEncoder.deliverWriteToken(app, producer, owner, labels, ttl, getKeyStore())

        // decode it with the token filter
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())
        WriteToken tokenDecoded = tokenFilter.getWriteToken(writeToken)

        // asserts token values
        assert ttl == ((long) tokenDecoded.expiryTimestamp - (long) tokenDecoded.issuanceTimestamp)

        // this token belongs to this app and producer
        tokenDecoded.appName.equals(app)
        assert Arrays.equals(tokenDecoded.ownerId, uuidToBinaryByteArray(owner))
        assert Arrays.equals(tokenDecoded.producerId, uuidToBinaryByteArray(producer))

        // fixed labels on write
        assert tokenDecoded.labels.equals(labels)

        // no indices
        assert tokenDecoded.indices == null
    }

    //
    // Expected Errors
    //
    @Test
    void testTokenExpired() {
        // token with a validity of 1ms, its quite short
        String uuid = UUID.randomUUID().toString()
        String writeToken = tokenEncoder.deliverWriteToken("app", uuid, 1, getKeyStore())

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenExpired.class) {
            Thread.sleep(1)
            tokenFilter.getWriteToken(writeToken)
        }
    }

    @Test
    void testNoToken() {
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarNoToken.class) {
            tokenFilter.getWriteToken(null)
        }

        shouldFail(QuasarNoToken.class) {
            tokenFilter.getWriteToken("")
        }
    }

    //
    // Token Corruption Errors
    //
    @Test
    void testTokenAESCorrupted() {
        // generate a token
        String uuid = UUID.randomUUID().toString()
        String writeToken = tokenEncoder.deliverWriteToken("app", uuid, 32478, getKeyStore())

        // corrupt the token (pick a random character and decrement it)
        char c1 = writeToken.charAt(new Random().nextInt(60))
        char c2 = c1 - 1
        writeToken = writeToken.replace(c1, c2)

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenInvalid.class) {
            tokenFilter.getWriteToken(writeToken)
        }
    }

    @Test
    void testTokenB64Corrupted() {
        // generate a token
        String uuid = UUID.randomUUID().toString()
        String writeToken = tokenEncoder.deliverWriteToken("app", uuid, 32478, getKeyStore())

        // corrupt the token (replace a by A)
        writeToken = "&~#{[]}".concat(writeToken)

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenInvalid.class) {
            tokenFilter.getWriteToken(writeToken)
        }
    }

    @Test
    void testBadTokenType() {
        String uuid = UUID.randomUUID().toString()
        String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, ["app"], 32468, getKeyStore())

        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())

        shouldFail(QuasarTokenInvalid.class) {
            tokenFilter.getWriteToken(readToken)
        }
    }
}
