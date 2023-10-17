//
//   Copyright 2018-2023  SenX S.A.S.
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

package io.warp10.crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.Argon2Parameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

import com.google.common.primitives.Longs;

import java.util.Arrays;

/**
 * The type Crypto utils.
 */
public class CryptoUtils {

  private static final String ARGON2_ITERATIONS = "argon2.iterations";
  private static final String ARGON2_ITERATIONS_DEFAULT = Integer.toString(3);
  private static final String ARGON2_MEMORY = "argon2.memory";
  private static final String ARGON2_MEMORY_DEFAULT = Integer.toString(524288);
  private static final String ARGON2_PARALLELISM = "argon2.parallelism";
  private static final String ARGON2_PARALLELISM_DEFAULT = Integer.toString(1);

  private static final long SALT_K0 = 0x9D38769AE67064E8L;
  private static final long SALT_K1 = 0x880EE777C5AEEFDDL;

  private static final long SECRET_K0 = 0xAD5440600A88E93FL;
  private static final long SECRET_K1 = 0xBF2299D5BC38C882L;

  private static final long ADDITIONAL_K0 = 0x5BCDAA0E77F14C90L;
  private static final long ADDITIONAL_K1 = 0x0511E948D63E47B8L;

  /**
   * Wrap byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] wrap(byte[] key, byte[] data) {
    AESWrapEngine engine = new AESWrapEngine();
    KeyParameter params = new KeyParameter(key);
    engine.init(true, params);
    PKCS7Padding padding = new PKCS7Padding();
    byte[] unpadded = data;

    //
    // Add padding
    //

    byte[] padded = new byte[unpadded.length + (8 - unpadded.length % 8)];
    System.arraycopy(unpadded, 0, padded, 0, unpadded.length);
    padding.addPadding(padded, unpadded.length);

    //
    // Wrap
    //

    byte[] encrypted = engine.wrap(padded, 0, padded.length);

    return encrypted;
  }

  /**
   * Unwrap byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] unwrap(byte[] key, byte[] data) {
    //
    // Decrypt the encrypted data
    //

    AESWrapEngine engine = new AESWrapEngine();
    CipherParameters params = new KeyParameter(key);
    engine.init(false, params);

    try {
      byte[] decrypted = engine.unwrap(data, 0, data.length);
      //
      // Unpad the decrypted data
      //

      PKCS7Padding padding = new PKCS7Padding();
      int padcount = padding.padCount(decrypted);

      //
      // Remove padding
      //

      decrypted = Arrays.copyOfRange(decrypted, 0, decrypted.length - padcount);

      return decrypted;
    } catch (InvalidCipherTextException icte) {
      return null;
    }
  }

  /**
   * Add mac byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] addMAC(long[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key[0], key[1], data);

    byte[] authenticated = Arrays.copyOf(data, data.length + 8);
    System.arraycopy(Longs.toByteArray(hash), 0, authenticated, data.length, 8);

    return authenticated;
  }

  /**
   * Add mac byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] addMAC(byte[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key, data);

    byte[] authenticated = Arrays.copyOf(data, data.length + 8);
    System.arraycopy(Longs.toByteArray(hash), 0, authenticated, data.length, 8);

    return authenticated;
  }

  /**
   * Check the MAC and return the verified content or null if the verification failed
   *
   * @param key  the key
   * @param data the data
   * @return byte [ ]
   */
  public static byte[] removeMAC(byte[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key, data, 0, data.length - 8);
    long mac = Longs.fromByteArray(Arrays.copyOfRange(data, data.length - 8, data.length));

    if (mac == hash) {
      return Arrays.copyOf(data, data.length - 8);
    } else {
      return null;
    }
  }

  /**
   * Remove mac byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] removeMAC(long[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key[0], key[1], data, 0, data.length - 8);
    long mac = Longs.fromByteArray(Arrays.copyOfRange(data, data.length - 8, data.length));

    if (mac == hash) {
      return Arrays.copyOf(data, data.length - 8);
    } else {
      return null;
    }
  }

  /**
   * Compute a safe SipHash of a given input by computing the
   * forward hash, the hash of the reversed input and finally the
   * hash of the two concatenated hashes.
   * <p>
   * This should prevent having meaningful collisions. If two contents have colliding hashes,
   * this means that the concatenation of their forward and reverse hashes are collisions for SipHah, quite unlikely.
   *
   * @param k0  the k 0
   * @param k1  the k 1
   * @param msg the msg
   * @return long
   */
  public static long safeSipHash(long k0, long k1, byte[] msg) {
    byte[] a = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(a).order(ByteOrder.BIG_ENDIAN);

    bb.putLong(SipHashInline.hash24(k0, k1, msg, 0, msg.length));
    bb.putLong(SipHashInline.hash24(k0, k1, msg, 0, msg.length, true));

    return SipHashInline.hash24(k0, k1, a, 0, a.length, false);
  }

  /**
   * Invert byte [ ].
   *
   * @param key the key
   * @return the byte [ ]
   */
  public static byte[] invert(byte[] key) {
    byte[] inverted = Arrays.copyOf(key, key.length);
    for (int i = 0; i < inverted.length; i++) {
      inverted[i] = (byte) (inverted[i] & 0xFF);
    }
    return inverted;
  }

  public static byte[] decodeKey(KeyStore ks, String encoded) {
    if (null == encoded) {
      return null;
    }

    if (encoded.startsWith("hex:")) {
      return Hex.decode(encoded.substring(4));
    } else if (encoded.startsWith("base64:")) {
      return Base64.decode(encoded.substring(7));
    } else if (encoded.startsWith("argon2id:")) {
      // Extract the bit size
      encoded = encoded.substring("argon2id:".length());
      String bitstr = encoded.replaceAll(":.*", "");
      int bitsize = Integer.valueOf(bitstr);

      // Retrieve the decoded key
      byte[] key = decodeKey(ks, encoded.substring(bitstr.length() + 1));

      // Apply the KDF
      int iters = Integer.valueOf(System.getProperty(ARGON2_ITERATIONS, ARGON2_ITERATIONS_DEFAULT));
      int memory = Integer.valueOf(System.getProperty(ARGON2_MEMORY, ARGON2_MEMORY_DEFAULT));
      int parallelism = Integer.valueOf(System.getProperty(ARGON2_PARALLELISM, ARGON2_PARALLELISM_DEFAULT));

      // Allocate 16 bytes for salt as this is recommended by RFC-9106
      byte[] salt = new byte[16];
      System.arraycopy(Longs.toByteArray(SipHashInline.hash24_palindromic(SALT_K0, SALT_K1, key)), 0, salt, 0, 8);
      System.arraycopy(Longs.toByteArray(SipHashInline.hash24_palindromic(SALT_K1, SALT_K0, key)), 0, salt, 8, 8);

      byte[] secret = Longs.toByteArray(SipHashInline.hash24_palindromic(SECRET_K0, SECRET_K1, key));
      byte[] additional = Longs.toByteArray(SipHashInline.hash24_palindromic(ADDITIONAL_K0, ADDITIONAL_K1, key));

      Argon2Parameters.Builder builder = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
          .withSalt(salt)
          .withSecret(secret)
          .withAdditional(additional)
          .withVersion(Argon2Parameters.ARGON2_VERSION_13)
          .withIterations(iters)
          .withMemoryAsKB(memory)
          .withParallelism(parallelism);
      Argon2BytesGenerator generator = new Argon2BytesGenerator();
      generator.init(builder.build());

      byte[] derived = new byte[bitsize >>> 3];
      generator.generateBytes(key, derived);

      return derived;
    } else {
      return ks.decodeKey(encoded);
    }
  }

  public static byte[] decodeSimpleKey(String encoded) {
    if (null == encoded) {
      return null;
    }

    if (encoded.startsWith("hex:")) {
      return Hex.decode(encoded.substring(4));
    } else if (encoded.startsWith("base64:")) {
      return Base64.decode(encoded.substring(7));
    } else {
      return null;
    }
  }
}
