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

package io.warp10.script.lora;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Hex;

import com.google.common.base.Charsets;

/**
 * Encrypt a LoRaWAN payload
 * 
 * @see https://github.com/Lora-net/LoRaMac-node/blob/master/src/mac/LoRaMacCrypto.c
 *
 * @param Message (in hex)
 * @param Address (LONG)
 * @param Direction (LONG)
 * @param SequenceNumber (LONG)
 * @param [top] Key (in hex)
 */
public class LORAENC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LORAENC(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a 128 bits hex encoded key on top of the stack.");
    }
    
    String keystr = top.toString();
    
    if (keystr.length() != 32) {
      throw new WarpScriptException(getName() + " expects a 128 bits hex encoded key on top of the stack.");
    }
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a sequence counter below the key.");
    }
    
    int sequenceCounter = ((Number) top).intValue();
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a direction (0 uplink or 1 downlink) below the sequence counter.");
    }
    
    int dir = ((Number) top).intValue();
    
    if (0 != dir && 1 != dir) {
      throw new WarpScriptException(getName() + " expects a direction (0 uplink or 1 downlink) below the sequence counter.");
    }
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a device address below the direction.");
    }
    
    int addr = ((Number) top).intValue();
        
    String datastr = stack.pop().toString();
    
    if (0 != datastr.length() % 2) {
      throw new WarpScriptException(getName() + " expects a hex encoded data frame with an even length of hex digits.");
    }
    
    byte[] data = Hex.decode(datastr);    
    
    //
    // Compute MIC block B0
    //
    
    byte[] ABlock = new byte[16];
    
    ABlock[0] = 0x01;
    ABlock[5] = (byte) (dir & 0x1);
    ABlock[6] = (byte) (addr & 0xFF);
    ABlock[7] = (byte) ((addr >> 8 ) & 0xFF);
    ABlock[8] = (byte) ((addr >> 16 ) & 0xFF);
    ABlock[9] = (byte) ((addr >> 24 ) & 0xFF);

    ABlock[10] = (byte) ((sequenceCounter ) & 0xFF);
    ABlock[11] = (byte) ((sequenceCounter >> 8 ) & 0xFF);
    ABlock[12] = (byte) ((sequenceCounter >> 16 ) & 0xFF);
    ABlock[13] = (byte) ((sequenceCounter >> 24 ) & 0xFF);

    int nblocks = data.length / 16 + (0 == data.length % 16 ? 0 : 1);
    
    AESEngine aes = new AESEngine();
    KeyParameter key = new KeyParameter(Hex.decode(keystr));
    aes.init(true, key);
    
    byte[] SBlock = new byte[16];

    int offset = 0;
    
    for (int i = 0; i < nblocks; i++) {
      ABlock[15] = (byte) (i & 0xFF);
      aes.reset();
      aes.processBlock(ABlock, 0, SBlock, 0);
    
      for (int k = 0; i < 16; i++) {
        if (offset + k < data.length) {
          data[offset + k] = (byte) (data[offset + k] ^ SBlock[k]);          
        }
      }
      offset += 16;
    }
    
    stack.push(new String(Hex.encode(data), Charsets.US_ASCII));
    
    return stack;
  }
}
