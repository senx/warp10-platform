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

import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.boon.json.JsonSerializer;
import org.boon.json.JsonSerializerFactory;

import com.google.common.base.Charsets;

public class LogUtil {
  
  public static final String EVENT_CREATION = "event.creation";
  public static final String WARPSCRIPT_SCRIPT = "warpscript.script";
  public static final String WARPSCRIPT_TIMES = "warpscript.times";
  public static final String STACK_TRACE = "stack.trace";
    
  public static final String DELETION_TOKEN = "deletion.token";
  public static final String DELETION_SELECTOR = "deletion.selector";
  public static final String DELETION_START = "deletion.start";
  public static final String DELETION_END = "deletion.end";
  public static final String DELETION_METADATA = "deletion.metadata";
  public static final String DELETION_GTS = "deletion.gts";
  public static final String DELETION_COUNT = "deletion.count";
  
  private static boolean checkedAESKey = false;
  private static byte[] loggingAESKey = null;
  
  /**
   * Set the attribute of a logging event
   */
  public static final LoggingEvent setLoggingEventAttribute(LoggingEvent event, String name, String value) {

    event = ensureLoggingEvent(event);
    
    if (null != name && null != value) {
      event.putToAttributes(name, value);
    }
    
    return event;
  }
  
  public static final String serializeLoggingEvent(KeyStore keystore, LoggingEvent event) {
    if (null == event) {
      return null;
    }
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    byte[] serialized = null;
    
    try {
      serialized = serializer.serialize(event);
    } catch (TException te) {
      return null;
    }
    
    if (!checkedAESKey) {
      checkedAESKey = true;
      loggingAESKey = keystore.getKey(KeyStore.AES_LOGGING);      
    }
    if (null != loggingAESKey) {
      serialized = CryptoUtils.wrap(loggingAESKey, serialized);
    }
    
    return new String(OrderPreservingBase64.encode(serialized), Charsets.US_ASCII); 
  }
  
  public static final LoggingEvent setLoggingEventStackTrace(LoggingEvent event, String name, Throwable t) {
    
    event = ensureLoggingEvent(event);
    
    // Fill the stack trace
    t.fillInStackTrace();
    
    StackTraceElement[] ste = t.getStackTrace();

    Object[][] stacktrace = new Object[ste.length][];
    
    for (int i = 0; i < ste.length; i++) {
      stacktrace[i] = new Object[4];
      stacktrace[i][0] = ste[i].getFileName();
      stacktrace[i][1] = ste[i].getLineNumber();
      stacktrace[i][2] = ste[i].getClassName();
      stacktrace[i][3] = ste[i].getMethodName();
    }
    
    if (null == event) {
      event = new LoggingEvent();
    }
    
    JsonSerializer ser = new JsonSerializerFactory().create();    

    event.putToAttributes(name, ser.serialize(stacktrace).toString());
    
    return event;
  }
  
  private static final LoggingEvent ensureLoggingEvent(LoggingEvent event) {
    if (null == event) {
      event = new LoggingEvent();
    }

    if (0 == event.getAttributesSize() || !event.getAttributes().containsKey(EVENT_CREATION)) {
      event.putToAttributes(EVENT_CREATION, Long.toString(System.currentTimeMillis()));
    }
    
    return event;
  }
  
  public static final LoggingEvent unwrapLog(byte[] key, String logmsg) {    
    try {
      byte[] data = OrderPreservingBase64.decode(logmsg.getBytes(Charsets.US_ASCII));
      
      if (null == data) {
        return null;      
      }
      
      data = CryptoUtils.unwrap(key, data);
      
      if (null == data) {
        return null;
      }
      
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      LoggingEvent event = new LoggingEvent();
      try {
        deserializer.deserialize(event, data);
      } catch (TException te) {
        return null;
      }
      
      return event;      
    } catch (Exception e) {
      return null;
    }
  }  
}
