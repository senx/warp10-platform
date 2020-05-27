//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.standalone;

public class AcceleratorConfig {

  public static final String ATTR_NOCACHE = "accel.nocache";
  public static final String ATTR_NOPERSIST = "accel.nopersist";
  public static final String NOCACHE = "nocache";
  public static final String NOPERSIST = "nopersist";
  public static final String CACHE = "cache";
  public static final String PERSIST = "persist";
  public static final String ACCELERATOR_HEADER = "X-Warp10-Accelerator";

  static boolean defaultWriteNocache = false;
  static boolean defaultWriteNopersist = false;
  static boolean defaultDeleteNocache = false;
  static boolean defaultDeleteNopersist = false;
  static boolean defaultReadNocache = false;
  static boolean defaultReadNopersist = false;
  
  private static int chunkCount = 0;
  private static long chunkSpan = 0L;
  
  /**
   * Is the Accelerator on (configured) or not
   */
  private static boolean instantiated = false;
  
  /**
   * Was the last FETCH accelerated for the given Thread?
   */
  static final ThreadLocal<Boolean> accelerated = new ThreadLocal<Boolean>() {
    protected Boolean initialValue() {
      return Boolean.FALSE;
    };
  };
  
  static final ThreadLocal<Boolean> nocache = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };
  
  static final ThreadLocal<Boolean> nopersist = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };
  
  public static final boolean getDefaultReadNocache() {
    return defaultReadNocache;
  }
  
  public static final boolean getDefaultReadNopersist() {
    return defaultReadNopersist;
  }
  public static final boolean getDefaultWriteNocache() {
    return defaultWriteNocache;
  }
  
  public static final boolean getDefaultWriteNopersist() {
    return defaultWriteNopersist;
  }
  
  public static final boolean getDefaultDeleteNocache() {
    return defaultDeleteNocache;
  }
  
  public static final boolean getDefaultDeleteNopersist() {
    return defaultDeleteNopersist;
  }

  public static void instantiated() {
    instantiated = true;
  }

  public static final void nocache() {
    if (instantiated) {
      nocache.set(Boolean.TRUE);
    }
  }
  
  public static final void cache() {
    if (instantiated) {
      nocache.set(Boolean.FALSE);
    }
  }
  
  public static final void nopersist() {
    if (instantiated) {
      nopersist.set(Boolean.TRUE);
    }
  }
  
  public static final void persist() {
    if (instantiated) {   
      nopersist.set(Boolean.FALSE);
    }
  }
  
  public static final boolean isCache() {
    if (instantiated) {
      return !nocache.get();
    } else {
      return false;
    }
  }
  
  public static final boolean isPersist() {
    if (instantiated) {
      return !nopersist.get();
    } else {
      return true;
    }
  }
  
  public static final boolean accelerated() {
    if (instantiated) {
      return accelerated.get();
    } else {
      return false;
    }
  }
  
  public static final boolean isInstantiated() {
    return instantiated;
  }
  
  public static void setChunkCount(int count) {
    chunkCount = count;
  }
  
  public static void setChunkSpan(long span) {
    chunkSpan = span;
  }
  
  public static int getChunkCount() {
    return chunkCount;
  }
  
  public static long getChunkSpan() {
    return chunkSpan;
  }
}
