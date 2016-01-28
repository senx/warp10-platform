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

package io.warp10.worf;


import com.google.common.base.Strings;
import io.warp10.quasar.token.thrift.data.TokenType;

import java.io.PrintWriter;

public class EncodeTokenCommand extends TokenCommand {

  public EncodeTokenCommand() {
    this.commandName = "encodeToken";
  }

  public TokenType tokenType = null;
  public String application = null;
  public String owner = null;
  public String producer = null;
  public long ttl = 0L;

  @Override
  public boolean isReady() {
    return !Strings.isNullOrEmpty(application) && !Strings.isNullOrEmpty(owner) && !Strings.isNullOrEmpty(producer) && ttl != 0 && tokenType != null;
  }

  @Override
  public boolean execute(String function, WorfKeyMaster worfKeyMaster, PrintWriter out) throws WorfException {
    boolean returnValue = false;
    switch (function) {
      case "generate":
        String token = null;
        switch (tokenType) {
          case READ:
            token = worfKeyMaster.deliverReadToken(application, producer, owner, ttl);
            break;
          case WRITE:
            token = worfKeyMaster.deliverWriteToken(application, producer, owner, ttl);
            break;
        }

        out.println("token=" + token);
        out.println("tokenIdent=" + worfKeyMaster.getTokenIdent(token));
        out.println("application name=" + application);
        out.println("producer & owner=" + producer + " & " + owner);
        out.println("ttl=" + ttl);

        // reset current command
        returnValue = true;
        break;
    }

    return returnValue;
  }
}
