//
//   Copyright 2025  SenX S.A.S.
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

package io.warp10.script.functions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

import io.warp10.BytesUtils;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.StoreClient.KVIterator;
import io.warp10.continuum.store.thrift.data.KVFetchRequest;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class KVLOAD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String KEY_START = "start";
  public static final String KEY_END = "end";
  public static final String KEY_KEYS = "keys";
  public static final String KEY_TOKEN = "token";
  public static final String KEY_MACRO = "macro";
  public static final String KEY_KFORMAT = "kformat";
  public static final String KEY_VFORMAT = "vformat";

  private static final FORMAT DEFAULT_KFORMAT = FORMAT.OPB64;
  private static final FORMAT DEFAULT_VFORMAT = FORMAT.RAW;

  private static enum FORMAT {
    RAW,
    UTF8,
    ISO88591,
    OPB64,
    HEX,
    B64,
  };

  public KVLOAD(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " operates on a map.");
    }

    Map<Object,Object> params = (Map<Object,Object>) top;

    if (!(params.get(KEY_TOKEN) instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token under '" + KEY_TOKEN + "'.");
    }

    //
    // If the token has an attribute .kvprefix then the call can succeed. This attribute
    // acts as a capability.
    //

    ReadToken rtoken = Tokens.extractReadToken((String) params.get(KEY_TOKEN));

    KVFetchRequest kvfr = new KVFetchRequest();

    kvfr.setToken(rtoken);

    if (params.get(KEY_KEYS) instanceof List) {
      List<Object> keys = (List<Object>) params.get(KEY_KEYS);
      List<byte[]> bkeys = new ArrayList<byte[]>(keys.size());

      for (Object key: keys) {
        if (key instanceof byte[]) {
          bkeys.add((byte[]) key);
        } else if (key instanceof String) {
          byte[] k = ((String) key).getBytes(StandardCharsets.UTF_8);
          if (k.length != ((String) key).length()) {
            throw new WarpScriptException(getName() + " STRING keys cannot contain non ISO-8859-1 characters.");
          }
          bkeys.add(k);
        } else {
          throw new WarpScriptException(getName() + " expects '" + KEY_KEYS + "' to contain a list of BYTES or STRING keys.");
        }
      }

      // Sort the list in lexicographical order to speed up retrieval

      bkeys.sort(new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
          return BytesUtils.compareTo(o1, o2);
        }
      });

      for (byte[] key: bkeys) {
        kvfr.addToKeys(ByteBuffer.wrap(key));
      }
    } else {
      byte[] start = null;
      byte[] end = null;

      if (params.get(KEY_START) instanceof byte[]) {
        start = Arrays.copyOf((byte[]) params.get(KEY_START), ((byte[]) params.get(KEY_START)).length);
      } else if (params.get(KEY_START) instanceof String) {
        start = ((String) params.get(KEY_START)).getBytes(StandardCharsets.UTF_8);
        if (start.length != ((String) params.get(KEY_START)).length()) {
          throw new WarpScriptException(getName() + " STRING keys cannot contain non ISO-8859-1 characters.");
        }
      } else {
        throw new WarpScriptException(getName() + " expects keys to be STRING or BYTES.");
      }

      if (params.get(KEY_END) instanceof byte[]) {
        end = Arrays.copyOf((byte[]) params.get(KEY_END), ((byte[]) params.get(KEY_END)).length);
      } else if (params.get(KEY_END) instanceof String) {
        end = ((String) params.get(KEY_END)).getBytes(StandardCharsets.UTF_8);
        if (end.length != ((String) params.get(KEY_END)).length()) {
          throw new WarpScriptException(getName() + " STRING keys cannot contain non ISO-8859-1 characters.");
        }
      } else {
        throw new WarpScriptException(getName() + " expects keys to be STRING or BYTES.");
      }

      kvfr.setStart(start);
      kvfr.setStop(end);
    }

    StoreClient store = stack.getStoreClient();

    Map<Object,Object> kv = new LinkedHashMap<Object,Object>();

    FORMAT kformat = FORMAT.valueOf(String.valueOf(params.getOrDefault(KEY_KFORMAT, DEFAULT_KFORMAT.name())).replaceAll("[^a-zA-z0-9]", "").toUpperCase());
    FORMAT vformat = FORMAT.valueOf(String.valueOf(params.getOrDefault(KEY_VFORMAT, DEFAULT_VFORMAT.name())).replaceAll("[^a-zA-z0-9]", "").toUpperCase());

    Macro macro = null;

    if (params.get(KEY_MACRO) instanceof Macro) {
      macro = (Macro) params.get(KEY_MACRO);
    }

    try (KVIterator<Entry<byte[],byte[]>> iter = store.kvfetch(kvfr)) {
      while(iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();

        if (null != macro) {
          stack.push(entry.getKey());
          stack.push(entry.getValue());
          stack.exec(macro);
          if (!Boolean.TRUE.equals(stack.pop())) {
            continue;
          }
        }

        Object key = entry.getKey();

        switch(kformat) {
          case RAW:
            break;
          case HEX:
            key = Hex.toHexString(entry.getKey());
            break;
          case ISO88591:
            key = new String(entry.getKey(), StandardCharsets.ISO_8859_1);
            break;
          case UTF8:
            key = new String(entry.getKey(), StandardCharsets.UTF_8);
            break;
          case B64:
            key = Base64.toBase64String(entry.getKey());
            break;
          case OPB64:
            key = OrderPreservingBase64.encodeToString(entry.getKey());
            break;
          default:
            key = OrderPreservingBase64.encodeToString(entry.getKey());
        }

        Object value = entry.getValue();

        switch(vformat) {
          case RAW:
            break;
          case HEX:
            value = Hex.toHexString(entry.getValue());
            break;
          case ISO88591:
            value = new String(entry.getValue(), StandardCharsets.ISO_8859_1);
            break;
          case UTF8:
            value = new String(entry.getValue(), StandardCharsets.UTF_8);
            break;
          case B64:
            value = Base64.toBase64String(entry.getValue());
            break;
          case OPB64:
            value = OrderPreservingBase64.encodeToString(entry.getValue());
            break;
        }

        kv.put(key, value);
      }
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an error while reading Key/Value pairs.", e);
    }

    stack.push(kv);

    return stack;
  }
}
