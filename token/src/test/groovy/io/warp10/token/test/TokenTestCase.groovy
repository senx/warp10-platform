//
//   Copyright 2016  Cityzen Data
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

import io.warp10.crypto.KeyStore
import io.warp10.crypto.UnsecureKeyStore
import io.warp10.quasar.filter.QuasarConfiguration
import org.junit.Ignore

import java.nio.ByteBuffer
import java.nio.ByteOrder

@Ignore
class TokenTestCase extends GroovyTestCase {

    protected KeyStore getKeyStore() {
        // Create an unsecured  keystore
        def keystore = new UnsecureKeyStore()

        // SET THE KEYS NEEDED BY THE TOKEN FILTER
        keystore.setKey(KeyStore.SIPHASH_TOKEN, keystore.decodeKey('hex:00000000000000000000000000000000'));
        keystore.setKey(KeyStore.SIPHASH_APPID, keystore.decodeKey('hex:00000000000000000000000000000001'));
        keystore.setKey(KeyStore.AES_TOKEN, keystore.decodeKey('hex:00000000000000000000000000000002'));

        return keystore
    }

    protected Properties getConfig() {
        Properties conf = new Properties()

        conf.setProperty(QuasarConfiguration.WARP_TRL_PATH, "./trl")
        conf.setProperty(QuasarConfiguration.WARP_TRL_PERIOD, Integer.toString(500))
        conf.setProperty(QuasarConfiguration.WARP_TRL_STARTUP_DELAY, Integer.toString(5000))
        return conf
    }

    protected ByteBuffer uuidToByteBuffer(String strUUID) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.BIG_ENDIAN)

        UUID uuid = UUID.fromString(strUUID)

        buffer.putLong(uuid.mostSignificantBits)
        buffer.putLong(uuid.leastSignificantBits)

        buffer.position(0)
        return buffer
    }

    protected List<ByteBuffer> uuidToByteBuffer(List<String> uuids) {
        List<ByteBuffer> out = []

        uuids.each { uuid ->
            out.add(uuidToByteBuffer(uuid))
        }

        return out
    }

    protected byte[] uuidToBinaryByteArray(String strUUID) {
        return uuidToByteBuffer(strUUID).array()
    }
}
