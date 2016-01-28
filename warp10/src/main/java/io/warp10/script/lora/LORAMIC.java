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
import org.bouncycastle.crypto.macs.CMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Hex;

import com.google.common.base.Charsets;

/**
 * Compute a LoRaWAN Message Integrity Code
 * 
 * @see https://github.com/Lora-net/LoRaMac-node/blob/master/src/mac/LoRaMacCrypto.c
 *
 * @param Message (in hex)
 * @param Address (LONG)
 * @param Direction (LONG)
 * @param SequenceNumber (LONG)
 * @param [top] Key (in hex)
 */
public class LORAMIC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public LORAMIC(String name) {
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
    
    byte[] MicBlockB0 = new byte[16];
    
    MicBlockB0[0] = 0x49;
    MicBlockB0[5] = (byte) (dir & 0x1);
    MicBlockB0[6] = (byte) (addr & 0xFF);
    MicBlockB0[7] = (byte) ((addr >> 8 ) & 0xFF);
    MicBlockB0[8] = (byte) ((addr >> 16 ) & 0xFF);
    MicBlockB0[9] = (byte) ((addr >> 24 ) & 0xFF);

    MicBlockB0[10] = (byte) ((sequenceCounter ) & 0xFF);
    MicBlockB0[11] = (byte) ((sequenceCounter >> 8 ) & 0xFF);
    MicBlockB0[12] = (byte) ((sequenceCounter >> 16 ) & 0xFF);
    MicBlockB0[13] = (byte) ((sequenceCounter >> 24 ) & 0xFF);

    MicBlockB0[15] = (byte) (data.length & 0xFF);
    
    
    AESEngine aes = new AESEngine();
    CMac cmac = new CMac(aes);
    KeyParameter key = new KeyParameter(Hex.decode(keystr));
    cmac.init(key);
    cmac.update(MicBlockB0, 0, MicBlockB0.length);
    cmac.update(data, 0, data.length & 0xFF);
    
    byte[] mac = new byte[cmac.getMacSize()];    
    cmac.doFinal(mac, 0);    
    
//    byte[] mic = new byte[4];
//    mic[0] = mac[3];
//    mic[1] = mac[2];
//    mic[2] = mac[1];
//    mic[3] = mac[0];
    
    
    stack.push(new String(Hex.encode(mac, 0, 4), Charsets.US_ASCII));
    
    return stack;
  }
}
