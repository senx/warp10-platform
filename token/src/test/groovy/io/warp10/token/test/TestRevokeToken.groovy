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
import io.warp10.quasar.filter.exception.QuasarTokenRevoked
import io.warp10.quasar.trl.QuasarTRL
import org.junit.Test

class TestRevokeToken extends TokenTestCase {
    private QuasarTokenEncoder tokenEncoder = new QuasarTokenEncoder()

    @Test
    void testRevokeToken() {
        // Generate 2 tokens
        String producer = UUID.randomUUID().toString()
        String app = "warp10.test"
        long ttl = 32468

        String readTokenValid = tokenEncoder.deliverReadToken(app, producer, producer, [app], ttl, getKeyStore())
        String readTokenRevoked = tokenEncoder.deliverReadToken(app, producer, producer, [app], ttl, getKeyStore())

        //instanciate a token filter
        QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore())
        // should work
        tokenFilter.getReadToken(readTokenValid)

        // get the token ident
        Long tokenIdent = tokenFilter.getTokenSipHash(readTokenRevoked.getBytes())
        // revoke it manually
        QuasarTRL trl = new QuasarTRL()
        trl.revokeToken(tokenIdent)
        tokenFilter.quasarTokenRevoked.onQuasarTRL(trl)

        // should fail
        shouldFail(QuasarTokenRevoked.class) {
            tokenFilter.getReadToken(readTokenRevoked)
        }
    }
}
