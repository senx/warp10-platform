//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;

public class ThriftUtils {
  public static TSerializer getTSerializer(TProtocolFactory factory) {
    try {
      return new TSerializer(factory);
    } catch (TException te) {
      throw new RuntimeException("Error while instantiating TSerializer.", te);
    }
  }

  public static TDeserializer getTDeserializer(TProtocolFactory factory) {
    try {
      return new TDeserializer(factory);
    } catch (TException te) {
      throw new RuntimeException("Error while instantiating TDeserializer.", te);
    }
  }
}
