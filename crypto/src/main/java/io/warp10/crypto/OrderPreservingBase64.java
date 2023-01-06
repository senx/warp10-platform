//
//   Copyright 2018-2022  SenX S.A.S.
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * This class implements a Base64 like encoding which preserves the lexicographic order of the
 * original byte arrays in the encoded ones.
 * <p>
 * This is needed so we can compare encoded byte arrays without having to first decode them
 */
public class OrderPreservingBase64 {

  /**
   * Alphabet preserving the ASCII lexicographic ordering.
   */
  private static final byte[] ALPHABET = ".0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.US_ASCII);

  private static final byte[] ALPHABET12 = new byte[8192];
  private static final byte[] TEBAHPLA = new byte[256];

  static {
    Arrays.fill(TEBAHPLA, (byte) -1);

    for (int i = 0; i < ALPHABET.length; i++) {
      TEBAHPLA[ALPHABET[i]] = (byte) i;
    }

    // Order Preserving Base 64 encoding substitutes an ascii character for 6 bits. 3 bytes input will generate 4 characters.
    // - The ALPHABET lookup table is 64 bits long. Once loaded in L1 cache, cpu will spend time to shift bits during encoding
    //   to process data per 6 bits chunks.
    // - The ALPHABET12 is a 8192 bits lookup table. It will also fit in L1 cache of most modern cpu (32KB since haswell)
    //   During encoding with ALPHABET12, cpu will do less bit shifting. Encoding per 12bits chunks doubles the speed. (now reaching 30MB/s on kaby lake)
    // Note that a 24 bit lookup table (~33MB) cannot fit in cache. Cache misses defeat the purpose of a lookup table, and encoding
    // will be a lot slower.
    //
    // ALPHABET12 is the list of two bytes representing the 12 bits input:
    // ...0.1.2.3.4.5.6  (...)  zmznzozpzqzrzsztzuzvzwzxzyzz
    // 12 bits at zero => output is '..'
    // 12 bits at one  => output is 'zz'
    for (int i = 0; i < 64; i++) {
      for (int j = 0; j < 64; j++) {
        ALPHABET12[(i * 64 + j) * 2] = ALPHABET[i];
        ALPHABET12[(i * 64 + j) * 2 + 1] = ALPHABET[j];
      }
    }
  }
  
  /**
   * Encode byte [ ].
   *
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] encode(byte[] data) {
    return encode(data, 0, data.length);
  }

  public static String encodeToString(byte[] data) {
    return new String(encode(data), StandardCharsets.UTF_8);
  }

  /**
   * Encode byte [ ].
   *
   * @param data   the data
   * @param offset the offset
   * @param datalen    the len
   * @return the byte [ ]
   */
  public static byte[] encode(byte[] data, int offset, int datalen) {
    int i = 0;
    int o = 0;
    
    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);
    
    byte[] encoded = new byte[len];
    
    int idx = 0;

    // first, process input per 3 bytes X Y Z
    int len24b = (datalen / 3) * 3; // length of 24 bits multiple
    for (i = offset; i < (offset + len24b); i += 3) {
      // take the first 12 bits (8 bits of X, 4 msb of Y), multiply by two to get the look up table index
      // ((((data[i]) << 4) | ((data[i + 1] & 0xF0) >>> 4)) << 1) & 0x1FFF is simplified into:
      o = (((data[i]) << 5) | ((data[i + 1] & 0xF0) >>> 3)) & 0x1FFF;
      encoded[idx++] = ALPHABET12[o];
      encoded[idx++] = ALPHABET12[o + 1];
      // take the next 12 bits (4 lsb bits of Y, 8 bits of Z), multiply by two to get the look up table index
      o = ((((data[i + 1]) << 8) | ((data[i + 2] & 0xFF))) << 1) & 0x1FFF;
      encoded[idx++] = ALPHABET12[o];
      encoded[idx++] = ALPHABET12[o + 1];
    }
    
    // then, encode last input bytes
    for (; i < (offset + datalen); i++) {
      switch ((i - offset) % 3) {
        case 0:
          encoded[idx++] = ALPHABET[(data[i] >> 2) & 0x3f];
          break;
        case 1:
          encoded[idx++] = ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)];
          break;
        case 2:
          encoded[idx++] = ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)];
          encoded[idx++] = ALPHABET[data[i] & 0x3f];
          break;
      }
    }
    
    // fill the last byte of output
    if (idx < encoded.length) {
      switch (datalen % 3) {
        case 1:
          encoded[idx] = ALPHABET[(data[offset + datalen - 1] << 4) & 0x30];
          break;
        case 2:
          encoded[idx] = ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c];
          break;
      }
    }
    return encoded;
  }

  /**
   * Encode to stream.
   *
   * @param data the data
   * @param out  the out
   * @throws IOException the io exception
   */
  public static void encodeToStream(byte[] data, OutputStream out) throws IOException {
    encodeToStream(out, data, 0, data.length);
  }

  /**
   * Encode to stream.
   *
   * @param out     the out
   * @param data    the data
   * @param offset  the offset
   * @param datalen the datalen
   * @throws IOException the io exception
   */
  public static void encodeToStream(OutputStream out, byte[] data, int offset, int datalen) throws IOException {
    
    int i = 0;
    int o = 0;

    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    // first, process input per 3 bytes X Y Z
    int len24b = (datalen / 3) * 3; // length of 24 bits multiple

    for (i = offset; i < (offset + len24b); i += 3) {
      // take the first 12 bits (8 bits of X, 4 msb of Y), multiply by two to get the look up table index
      // ((((data[i]) << 4) | ((data[i + 1] & 0xF0) >>> 4)) << 1) & 0x1FFF is simplified into:
      o = (((data[i]) << 5) | ((data[i + 1] & 0xF0) >>> 3)) & 0x1FFF;
      out.write(ALPHABET12[o]);
      out.write(ALPHABET12[o + 1]);
      // take the next 12 bits (4 lsb bits of Y, 8 bits of Z), multiply by two to get the look up table index
      o = ((((data[i + 1]) << 8) | ((data[i + 2] & 0xFF))) << 1) & 0x1FFF;
      out.write(ALPHABET12[o]);
      out.write(ALPHABET12[o + 1]);
    }

    int bufidx = 0;
    // then, encode last input bytes
    for (; i < (offset + datalen); i++) {
      switch (bufidx % 3) {
        case 0:
          out.write(ALPHABET[(data[i] >> 2) & 0x3f]);
          break;
        case 1:
          out.write(ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)]);
          break;
        case 2:
          out.write(ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)]);
          out.write(ALPHABET[data[i] & 0x3f]);
          break;
      }
      bufidx++;
    }

    // fill the last byte of output
    switch (datalen % 3) {
      case 1:
        out.write(ALPHABET[(data[offset + datalen - 1] << 4) & 0x30]);
        break;
      case 2:
        out.write(ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c]);
        break;
    }
  }

  /**
   * Encode to writer.
   *
   * @param out     the out
   * @param data    the data
   * @param offset  the offset
   * @param datalen the datalen
   * @param buflen  size of internal buffer
   * @throws IOException the io exception
   */
  public static void encodeToStream(OutputStream out, byte[] data, int offset, int datalen, int buflen) throws IOException {
    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    if (buflen < 1024) {
      buflen = 1024;
    }

    byte[] buf = new byte[buflen > len ? len : buflen];

    encodeToStream(out, data, offset, datalen, buf);
  }

  public static void encodeToStream(OutputStream out, byte[] data, int offset, int datalen, byte[] buf) throws IOException {

    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    int dataidx = 0;

    int bufidx = 0;

    int i = offset;
    int o = 0;

    // if buf len >= 4, we can fill it processing bytes 3 by 3 with ALPHABET12.
    if (buf.length >= 4) {
      int len24b = (datalen / 3) * 3; // length of 24 bits multiple
      for (i = offset; i < (offset + len24b); i += 3) {
        // take the first 12 bits (8 bits of X, 4 msb of Y), multiply by two to get the look up table index
        // ((((data[i]) << 4) | ((data[i + 1] & 0xF0) >>> 4)) << 1) & 0x1FFF is simplified into:
        o = (((data[i]) << 5) | ((data[i + 1] & 0xF0) >>> 3)) & 0x1FFF;
        buf[bufidx++] = ALPHABET12[o];
        buf[bufidx++] = ALPHABET12[o + 1];
        // take the next 12 bits (4 lsb bits of Y, 8 bits of Z), multiply by two to get the look up table index
        o = ((((data[i + 1]) << 8) | ((data[i + 2] & 0xFF))) << 1) & 0x1FFF;
        buf[bufidx++] = ALPHABET12[o];
        buf[bufidx++] = ALPHABET12[o + 1];
        if (buf.length - bufidx < 4) {
          out.write(buf, 0, bufidx);
          bufidx = 0;
        }
      }
      dataidx = (i > offset ? i - offset : 0);
    }


    for (; i < offset + datalen; i++) {
      // Ensure we have at least 2 slots free for case 2
      if (buf.length  - bufidx < 2) {
        out.write(buf, 0, bufidx);
        bufidx = 0;
      }
      switch (dataidx % 3) {
        case 0:
          buf[bufidx++] = ALPHABET[(data[i] >> 2) & 0x3f];
          break;
        case 1:
          buf[bufidx++] = ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)];
          break;
        case 2:
          buf[bufidx++] = ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)];
          buf[bufidx++] = ALPHABET[data[i] & 0x3f];
          break;
      }
      dataidx++;
    }

    if (bufidx > 0) {
      out.write(buf, 0, bufidx);
    }

    if (dataidx < len) {
      switch (datalen % 3) {
        case 1:
          out.write(ALPHABET[(data[offset + datalen - 1] << 4) & 0x30]);
          break;
        case 2:
          out.write(ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c]);
          break;
      }
    }
  }

  /**
   * Encode to writer.
   *
   * @param data the data
   * @param out  the out
   * @throws IOException the io exception
   */
  public static void encodeToWriter(byte[] data, Writer out) throws IOException {
    encodeToWriter(out, data, 0, data.length);
  }

  /**
   * Encode to writer.
   *
   * @param out     the out
   * @param data    the data
   * @param offset  the offset
   * @param datalen the datalen
   * @throws IOException the io exception
   */
  public static void encodeToWriter(Writer out, byte[] data, int offset, int datalen) throws IOException {
    // strict copy paste of encodeToStream... no common interface between Writer and OutputStream
    int i = 0;
    int o = 0;

    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    // first, process input per 3 bytes X Y Z
    int len24b = (datalen / 3) * 3; // length of 24 bits multiple

    for (i = offset; i < (offset + len24b); i += 3) {
      // take the first 12 bits (8 bits of X, 4 msb of Y), multiply by two to get the look up table index
      // ((((data[i]) << 4) | ((data[i + 1] & 0xF0) >>> 4)) << 1) & 0x1FFF is simplified into:
      o = (((data[i]) << 5) | ((data[i + 1] & 0xF0) >>> 3)) & 0x1FFF;
      out.write(ALPHABET12[o]);
      out.write(ALPHABET12[o + 1]);
      // take the next 12 bits (4 lsb bits of Y, 8 bits of Z), multiply by two to get the look up table index
      o = ((((data[i + 1]) << 8) | ((data[i + 2] & 0xFF))) << 1) & 0x1FFF;
      out.write(ALPHABET12[o]);
      out.write(ALPHABET12[o + 1]);
    }

    int bufidx = 0;
    // then, encode last input bytes
    for (; i < (offset + datalen); i++) {
      switch (bufidx % 3) {
        case 0:
          out.write(ALPHABET[(data[i] >> 2) & 0x3f]);
          break;
        case 1:
          out.write(ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)]);
          break;
        case 2:
          out.write(ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)]);
          out.write(ALPHABET[data[i] & 0x3f]);
          break;
      }
      bufidx++;
    }

    // fill the last byte of output
    switch (datalen % 3) {
      case 1:
        out.write(ALPHABET[(data[offset + datalen - 1] << 4) & 0x30]);
        break;
      case 2:
        out.write(ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c]);
        break;
    }
  }

  /**
   * Encode to writer.
   *
   * @param out     the out
   * @param data    the data
   * @param offset  the offset
   * @param datalen the datalen
   * @param buflen  size of internal buffer
   * @throws IOException the io exception
   */
  public static void encodeToWriter(Writer out, byte[] data, int offset, int datalen, int buflen) throws IOException {
    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    if (buflen < 1024) {
      buflen = 1024;
    }

    byte[] buf = new byte[buflen > len ? len : buflen];

    encodeToWriter(out, data, offset, datalen, buf);
  }

  public static void encodeToWriter(Writer out, byte[] data, int offset, int datalen, byte[] buf) throws IOException {

    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    int dataidx = 0;

    int bufidx = 0;

    int i = offset;
    int o = 0;

    // if buf len >= 4, we can fill it processing bytes 3 by 3 with ALPHABET12.
    if (buf.length >= 4) {
      int len24b = (datalen / 3) * 3; // length of 24 bits multiple
      for (i = offset; i < (offset + len24b); i += 3) {
        // take the first 12 bits (8 bits of X, 4 msb of Y), multiply by two to get the look up table index
        // ((((data[i]) << 4) | ((data[i + 1] & 0xF0) >>> 4)) << 1) & 0x1FFF is simplified into:
        o = (((data[i]) << 5) | ((data[i + 1] & 0xF0) >>> 3)) & 0x1FFF;
        buf[bufidx++] = ALPHABET12[o];
        buf[bufidx++] = ALPHABET12[o + 1];
        // take the next 12 bits (4 lsb bits of Y, 8 bits of Z), multiply by two to get the look up table index
        o = ((((data[i + 1]) << 8) | ((data[i + 2] & 0xFF))) << 1) & 0x1FFF;
        buf[bufidx++] = ALPHABET12[o];
        buf[bufidx++] = ALPHABET12[o + 1];
        if (buf.length - bufidx < 4) {
          out.write(new String(buf, 0, bufidx, StandardCharsets.US_ASCII));
          bufidx = 0;
        }
      }
      dataidx = i - offset;
    }

    for (; i < offset + datalen; i++) {
      // Ensure we have at least 2 slots free for case 2
      if (buf.length  - bufidx < 2) {
        out.write(new String(buf, 0, bufidx, StandardCharsets.US_ASCII));
        bufidx = 0;
      }
      switch (dataidx % 3) {
        case 0:
          buf[bufidx++] = ALPHABET[(data[i] >> 2) & 0x3f];
          break;
        case 1:
          buf[bufidx++] = ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)];
          break;
        case 2:
          buf[bufidx++] = ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)];
          buf[bufidx++] = ALPHABET[data[i] & 0x3f];
          break;
      }
      dataidx++;
    }

    if (bufidx > 0) {
      out.write(new String(buf, 0, bufidx, StandardCharsets.US_ASCII));
    }

    if (dataidx < len) {
      switch (datalen % 3) {
        case 1:
          out.write(ALPHABET[(data[offset + datalen - 1] << 4) & 0x30]);
          break;
        case 2:
          out.write(ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c]);
          break;
      }
    }
  }

  /**
   * Decode byte [ ].
   *
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] decode(byte[] data) {
    return decode(data, 0, data.length);
  }

  /**
   * Decode byte [ ].
   *
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the byte [ ]
   */
  public static byte[] decode(byte[] data, int offset, int len) {
    byte[] decoded = new byte[3 * (len / 4) + (len % 4 != 0 ? (len % 4 - 1) : 0)];

    int idx = 0;
    byte value = 0;

    for (int i = 0; i < len; i++) {
      switch (i % 4) {
        case 0:
          value = (byte) (TEBAHPLA[data[offset + i]] << 2);
          break;
        case 1:
          value |= (byte) ((TEBAHPLA[data[offset + i]] >> 4) & 0x3);
          decoded[idx++] = value;
          value = (byte) ((TEBAHPLA[data[offset + i]] << 4) & 0xf0);
          break;
        case 2:
          value |= (byte) ((TEBAHPLA[data[offset + i]] >> 2) & 0xf);
          decoded[idx++] = value;
          value = (byte) ((TEBAHPLA[data[offset + i]] << 6) & 0xc0);
          break;
        case 3:
          value |= TEBAHPLA[data[offset + i]];
          decoded[idx++] = value;
          break;
      }
    }

    // FIXME(hbs)
    if (idx < decoded.length) {
      decoded[idx++] = value;
    }

    return decoded;
  }

  public static byte[] decode(String data) {
    return decode(data, 0, data.length());
  }

  /**
   * Decode byte [ ].
   *
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the byte [ ]
   */
  public static byte[] decode(String data, int offset, int len) {
    byte[] decoded = new byte[3 * (len / 4) + (len % 4 != 0 ? (len % 4 - 1) : 0)];

    int idx = 0;
    byte value = 0;

    for (int i = 0; i < len; i++) {
      switch (i % 4) {
        case 0:
          value = (byte) (TEBAHPLA[data.charAt(offset + i)] << 2);
          break;
        case 1:
          value |= (byte) ((TEBAHPLA[data.charAt(offset + i)] >> 4) & 0x3);
          decoded[idx++] = value;
          value = (byte) ((TEBAHPLA[data.charAt(offset + i)] << 4) & 0xf0);
          break;
        case 2:
          value |= (byte) ((TEBAHPLA[data.charAt(offset + i)] >> 2) & 0xf);
          decoded[idx++] = value;
          value = (byte) ((TEBAHPLA[data.charAt(offset + i)] << 6) & 0xc0);
          break;
        case 3:
          value |= TEBAHPLA[data.charAt(offset + i)];
          decoded[idx++] = value;
          break;
      }
    }

    // FIXME(hbs)
    if (idx < decoded.length) {
      decoded[idx++] = value;
    }

    return decoded;
  }
}
