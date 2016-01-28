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

package io.warp10.quasar.encoder;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class QuasarTokenEncoder {

    private final TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    public String  deliverReadToken(String appName, String producerUID, String ownerUID, java.util.List<java.lang.String> apps,  long ttl, KeyStore keyStore) throws TException {
        long currentTime = System.currentTimeMillis();

        // Generate the READ Tokens
        ReadToken token = new ReadToken();
        token.setAppName(appName);
        token.setIssuanceTimestamp(currentTime);
        token.setExpiryTimestamp(currentTime + ttl);
        token.setTokenType(TokenType.READ);

        token.setApps(apps);
        token.addToOwners(toByteBuffer(ownerUID));
        token.setProducers(new ArrayList<ByteBuffer>());

        // Billing
        token.setBilledId(toByteBuffer(producerUID));
        return cypherToken(token, keyStore);
    }

    public String deliverWriteToken(String appName, String producerUID, String ownerUID, Map<String,String> labels, long ttl, KeyStore keystore) throws TException{
        long currentTime = System.currentTimeMillis();

        WriteToken token = new WriteToken();
        token.setAppName(appName);
        token.setIssuanceTimestamp(currentTime);
        token.setExpiryTimestamp(currentTime + ttl);
        token.setTokenType(TokenType.WRITE);

        if (labels != null && labels.size() >0) {
            token.setLabels(labels);
        }

        token.setProducerId(toByteBuffer(producerUID));
        token.setOwnerId(toByteBuffer(ownerUID));

        return cypherToken(token, keystore);
    }

    public String cypherToken(TBase<?, ?> token, KeyStore keyStore) throws TException {
        byte[] tokenAesKey = keyStore.getKey(KeyStore.AES_TOKEN);
        byte[] tokenSipHashkey = keyStore.getKey(KeyStore.SIPHASH_TOKEN);

        // Serialize the  thrift token into byte array
        byte[] serialized = serializer.serialize(token);

        // Calculate the SIP
        long sip = SipHashInline.hash24_palindromic(tokenSipHashkey, serialized);

        //Create the token byte buffer
        ByteBuffer buffer = ByteBuffer.allocate(8 + serialized.length);
        // adds the sip
        buffer.putLong(sip);
        // adds the thrift token
        buffer.put(serialized);

        // Wrap the TOKEN
        byte[] wrappedData = CryptoUtils.wrap(tokenAesKey, buffer.array());

        String accessToken = new String(OrderPreservingBase64.encode(wrappedData));

        return accessToken;
    }

    private ByteBuffer toByteBuffer(String strUUID) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.BIG_ENDIAN);

        UUID uuid = UUID.fromString(strUUID);

        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());

        buffer.position(0);
        return buffer;
    }
}
