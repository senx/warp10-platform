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

package io.warp10.continuum;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.geoxp.oss.CryptoHelper;
import com.geoxp.oss.client.OSSClient;
import com.geoxp.oss.jarjar.org.bouncycastle.util.encoders.Hex;

public class WarpInit {
  
  private static List<String> ACCESS_KEYS = new ArrayList<String>();
  
  static {
    ACCESS_KEYS.add("c8:1b:9c:fc:a9:f4:99:c1:6d:84:1f:6e:e9:dc:8f:77"); // Herberts
    ACCESS_KEYS.add("51:03:c4:98:ef:5d:d4:7c:03:45:19:38:1c:cb:1b:c9"); // Morin
    ACCESS_KEYS.add("6d:8c:fb:ad:c8:a7:2b:07:dc:f5:f9:9a:56:6a:cb:99"); // Lambour
    ACCESS_KEYS.add("42:30:d6:8e:67:68:6e:47:10:1f:52:57:f3:c0:a8:a5"); // Gonzalez
  }
  
  private static String[] SIPHASH_KEYS = {
    "continuum.hash.class",
    "continuum.hash.labels",
    "continuum.hash.index",
    "continuum.hash.token",
    "continuum.hash.app",
    "kafka.metadata.mac",
    "kafka.data.mac",
    "kafka.archive.mac",
    "directory.psk",
    "webcall.kafka.mac",
    "plasma.frontend.kafka.mac",
  };
  
  private static String[] AES_KEYS = {
    "continuum.aes.scripts",
    "continuum.aes.token",
    "continuum.aes.logging",
    "leveldb.metadata.aes",
    "leveldb.data.aes",
    "hbase.data.aes",
    "hbase.metadata.aes",
    "kafka.data.aes",
    "kafka.metadata.aes",
    "kafka.archive.aes",
    "webcall.kafka.aes",
    "plasma.frontend.kafka.aes",
  };  
  
  private static final SecureRandom sr = new SecureRandom();
  
  public static StringBuilder genCryptoKeys(String masterSecretName) throws Exception {
    StringBuilder sb = new StringBuilder();
    
    //
    // Generate OSS Master Key if ossURL is not null
    //
    
    String ossURL = System.getProperty("oss.url");
    
    byte[] masterKey = null;
    byte[] quasarKey = null;
    
    if (null != ossURL && null != masterSecretName) {
      System.out.println("# Generating master key");
      OSSClient.genSecret(ossURL, masterSecretName, null);
      if (!ACCESS_KEYS.isEmpty()) {
        OSSClient.addACL(ossURL, "c8:1b:9c:fc:a9:f4:99:c1:6d:84:1f:6e:e9:dc:8f:77", masterSecretName, ACCESS_KEYS);
      }
      masterKey = OSSClient.getSecret(ossURL, masterSecretName, null);
      quasarKey = OSSClient.getSecret(ossURL, "com.cityzendata.oss.quasar", null);
    }
    
    sb.append("##\n");
    sb.append("##    W A R N I N G\n");
    sb.append("##\n");
    sb.append("## COMMENT AES KEYS YOU DO NOT WANT TO ENFORCE\n");
    sb.append("##\n");
    sb.append("#################################################\n");
    sb.append("\n");
    sb.append("oss.master.key = ");
    sb.append(masterSecretName);
    sb.append("\n");
    
    byte[] siphashkey = new byte[16];
    
    System.out.println("# Generating SipHash keys");
    
    for (String key: SIPHASH_KEYS) {
      siphashkey = new byte[16];
      sr.nextBytes(siphashkey);
      
      sb.append(key);
      sb.append(" = ");

      if (null != masterKey) {
        siphashkey = CryptoHelper.wrapBlob(masterKey,siphashkey);
        sb.append("wrapped:");
      }
      
      sb.append("hex:");
      sb.append(new String(Hex.encode(siphashkey)));
      sb.append("\n");
      
      if (null != quasarKey && ("continuum.hash.token".equals(key) || "continuum.hash.app".equals(key))) {
        siphashkey = CryptoHelper.unwrapBlob(masterKey, siphashkey);
        siphashkey = CryptoHelper.wrapBlob(quasarKey, siphashkey);
        sb.append("quasar:");
        sb.append(key);
        sb.append(" = wrapped:");
        sb.append("hex:");
        sb.append(new String(Hex.encode(siphashkey)));
        sb.append("\n");
      }
    }

    System.out.println("# Generating AES keys");
    
    for (String key: AES_KEYS) {
      byte[] aeskey = new byte[32];
      sr.nextBytes(aeskey);
      
      sb.append(key);
      sb.append(" = ");

      if (null != masterKey) {
        aeskey = CryptoHelper.wrapBlob(masterKey,aeskey);
        sb.append("wrapped:");
      }
      
      sb.append("hex:");
      sb.append(new String(Hex.encode(aeskey)));
      sb.append("\n");
      
      if (null != quasarKey && "continuum.aes.token".equals(key)) {
        aeskey = CryptoHelper.unwrapBlob(masterKey, aeskey);
        aeskey = CryptoHelper.wrapBlob(quasarKey, aeskey);
        sb.append("quasar:");
        sb.append(key);
        sb.append(" = wrapped:");
        sb.append("hex:");
        sb.append(new String(Hex.encode(aeskey)));
        sb.append("\n");
      }
    }

    System.out.println("# Rewrapped keys for starbust/quasar");
    
    byte[] quasar = OSSClient.getSecret(ossURL, "com.cityzendata.oss.quasar", null);
    
    System.out.println("");
    return sb;
  }
}
