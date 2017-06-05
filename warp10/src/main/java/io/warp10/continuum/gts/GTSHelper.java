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

package io.warp10.continuum.gts;

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.SAXUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBinaryOp;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.MACROMAPPER;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.boon.json.JsonParser;
import org.boon.json.JsonParserFactory;

import sun.nio.cs.ArrayEncoder;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;


/**
 * Helper class to manipulate Geo Time Series.
 * 
 */
public class GTSHelper {

  /**
   * Maximum number of buckets we accept when bucketizing GTS instances.
   * 10M is a little less than 20 years at 1 minute resolution
   */
  private static final int MAX_BUCKETS = 10000000;
  
  /**
   * Maximum number of values for a GTS instance.
   */
  private static final int MAX_VALUES = 10000000;
    
  /**
   * Sort the values (and associated locations/elevations) by order of their ticks
   *  
   *  @param gts The GeoTimeSerie instance to sort
   *  @param reversed If true, the ticks will be sorted from most recent to least
   *  @return gts, only sorted
   */
  public static final GeoTimeSerie sort(GeoTimeSerie gts, boolean reversed) {
    
    //
    // If GTS is sorted in another order than the one requested,
    // consider it's not sorted.
    //
    
    if (gts.sorted && gts.reversed != reversed) {
      gts.sorted = false;
    }    

    //
    // Do not sort the GTS if it is already sorted as QS worst case is
    // encountered when the array is already sorted and is O(n^2)
    // 
    
    if (gts.sorted) {
      return gts;
    }
    
    quicksort(gts, 0, gts.values - 1, reversed);
    
    gts.sorted = true;
    gts.reversed = reversed;
        
    return gts;
  }
  
  /**
   * Sort the values (and associated locations/elevations) by order of ascending ticks.
   * 
   * @param gts GeoTimeSerie instance to sort
   * @return gts, only sorted
   */
  public static final GeoTimeSerie sort(GeoTimeSerie gts) {
    return sort(gts, false);
  }
  
  public static final GeoTimeSerie valueSort(GeoTimeSerie gts, boolean reversed) {
    quicksortByValue(gts, 0, gts.values - 1, reversed);
    return gts;
  }

  public static final GeoTimeSerie valueSort(GeoTimeSerie gts) {
    return valueSort(gts, false);
  }

  /**
   * Return an iterator on the GeoTimeSerie ticks.
   * 
   * If the GTS is bucketized, one tick per bucket is returned,
   * otherwise only the ticks for which there are values are returned.
   * 
   * If the GTS is not bucketized, the ticks are first sorted in natural ordering.
   * There is no tick deduplication.
   * 
   * 
   * @param gts GeoTimeSerie instance for which to return an iterator
   * @param reversed If true, ticks will be returned from most recent to oldest
   * @return
   */
  public static final Iterator<Long> tickIterator(GeoTimeSerie gts, final boolean reversed) {
    final GeoTimeSerie itergts = gts;
    
    if (!isBucketized(gts)) {
      
      //
      // GTS is not bucketized, return the ticks
      //
      
      sort(gts, false);
      
      return new Iterator<Long>() {

        int idx = reversed ? itergts.values - 1: 0;
        
        @Override
        public boolean hasNext() {
          return reversed ? (idx > 0) : (idx < itergts.values);
        }
        
        @Override
        public Long next() {
          return itergts.ticks[reversed ? idx-- : idx++];
        };
        @Override
        public void remove() {
          // NOOP          
        }
      };
    } else {
      
      //
      // GTS is bucketized
      //
      
      return new Iterator<Long>() {
        long bucket = reversed ? 0 : itergts.bucketcount - 1;
        
        @Override
        public boolean hasNext() {
          return reversed ? bucket < itergts.bucketcount : bucket >= 0;
        }
        @Override
        public Long next() {
          long ts = itergts.lastbucket - bucket * itergts.bucketspan;
          
          if (reversed) {
            bucket++;
          } else {
            bucket--;
          }
          
          return ts;
        }
        @Override
        public void remove() {
          // NOOP          
        }
      };
    }
  }
  
  /**
   * Sort a range of values/locations/elevations of a GeoTimeSerie instance
   * according to the ascending order of its ticks
   * 
   * @param gts GeoTimeSerie instance to sort
   * @param low Lowest index of range to sort
   * @param high Highest index of range to sort
   */
  private static final void quicksort(GeoTimeSerie gts, int low, int high, boolean reversed) { 
    
    if (0 == gts.values) {
      return;
    }
    
    int i = low, j = high;
    // Get the pivot element from the middle of the list
    long pivot = gts.ticks[low + (high-low)/2];

    // Divide into two lists
    while (i <= j) {
      // If the current value from the left list is smaller
      // (or greater if reversed is true) than the pivot
      // element then get the next element from the left list
      while ((!reversed && gts.ticks[i] < pivot) || (reversed && gts.ticks[i] > pivot)) {
        i++;
      }
      // If the current value from the right list is larger (or lower if reversed is true)
      // than the pivot element then get the next element from the right list
      while ((!reversed && gts.ticks[j] > pivot) || (reversed && gts.ticks[j] < pivot)) {
        j--;
      }

      // If we have found a values in the left list which is larger then
      // the pivot element and if we have found a value in the right list
      // which is smaller then the pivot element then we exchange the
      // values.
      // As we are done we can increase i and j
      if (i <= j) {
        long tmplong = gts.ticks[i];
        gts.ticks[i] = gts.ticks[j];
        gts.ticks[j] = tmplong;
        
        if (null != gts.locations) {
          tmplong = gts.locations[i];
          gts.locations[i] = gts.locations[j];
          gts.locations[j] = tmplong;          
        }
        
        if (null != gts.elevations) {
          tmplong = gts.elevations[i];
          gts.elevations[i] = gts.elevations[j];
          gts.elevations[j] = tmplong;          
        }
        
        if (TYPE.LONG == gts.type) {
          tmplong = gts.longValues[i];
          gts.longValues[i] = gts.longValues[j];
          gts.longValues[j] = tmplong;          
        } else if (TYPE.DOUBLE == gts.type) {
          double tmpdouble = gts.doubleValues[i];
          gts.doubleValues[i] = gts.doubleValues[j];
          gts.doubleValues[j] = tmpdouble;
        } else if (TYPE.STRING == gts.type) {
          String tmpstring = gts.stringValues[i];
          gts.stringValues[i] = gts.stringValues[j];
          gts.stringValues[j] = tmpstring;
        } else if (TYPE.BOOLEAN == gts.type) {
          boolean tmpboolean = gts.booleanValues.get(i);
          gts.booleanValues.set(i, gts.booleanValues.get(j));
          gts.booleanValues.set(j, tmpboolean);
        }

        i++;
        j--;
      }
    }
    
    // Recursion
    if (low < j) {
      quicksort(gts, low, j, reversed);
    }
    if (i < high) {   
      quicksort(gts, i, high, reversed);
    }
  }

  private static final void quicksortByValue(GeoTimeSerie gts, int low, int high, boolean reversed) { 
    
    if (0 == gts.values) {
      return;
    }
    
    int i = low, j = high;
    // Get the pivot element from the middle of the list
    long lpivot = 0L;
    double dpivot = 0.0D;
    String spivot = null;
    
    TYPE type = gts.getType();
    
    if (TYPE.LONG == type) {
      lpivot = gts.longValues[low + (high-low)/2];
    } else if (TYPE.DOUBLE == type) {
      dpivot = gts.doubleValues[low + (high-low)/2];
    } else if (TYPE.STRING == type) {
      spivot = gts.stringValues[low + (high-low)/2];       
    } else if (TYPE.BOOLEAN == type) {
      // Do nothing for booleans
      return;
    }
    
    long pivotTick = gts.ticks[low + (high-low) / 2];
    
    // Divide into two lists
    while (i <= j) {
      
      if (TYPE.LONG == type) {
        
        
        if (!reversed) {
          // If the current value from the left list is smaller
          // (or greater if reversed is true) than the pivot
          // element then get the next element from the left list        
          while(gts.longValues[i] < lpivot || (gts.longValues[i] == lpivot && gts.ticks[i] < pivotTick)) {
            i++;
          }
          // If the current value from the right list is larger (or lower if reversed is true)
          // than the pivot element then get the next element from the right list
          while(gts.longValues[j] > lpivot || (gts.longValues[j] == lpivot && gts.ticks[j] > pivotTick)) {
            j--;
          }
        } else {
          while(gts.longValues[i] > lpivot || (gts.longValues[i] == lpivot && gts.ticks[i] > pivotTick)) {
            i++;
          }
          while(gts.longValues[j] < lpivot || (gts.longValues[j] == lpivot && gts.ticks[j] < pivotTick)) {
            j--;
          }
        }
      } else if (TYPE.DOUBLE == type) {        
        if (!reversed) {
          // If the current value from the left list is smaller
          // (or greater if reversed is true) than the pivot
          // element then get the next element from the left list        
          while(gts.doubleValues[i] < dpivot || (gts.doubleValues[i] == dpivot && gts.ticks[i] < pivotTick)) {
            i++;
          }
          // If the current value from the right list is larger (or lower if reversed is true)
          // than the pivot element then get the next element from the right list
          while(gts.doubleValues[j] > dpivot || (gts.doubleValues[j] == dpivot && gts.ticks[j] > pivotTick)) {
            j--;
          }
        } else {
          while(gts.doubleValues[i] > dpivot || (gts.doubleValues[i] == dpivot && gts.ticks[i] > pivotTick)) {
            i++;
          }
          while(gts.doubleValues[j] < dpivot || (gts.doubleValues[j] == dpivot && gts.ticks[j] < pivotTick)) {
            j--;
          }
        }
      } else if (TYPE.STRING == type) {
        if (!reversed) {
          // If the current value from the left list is smaller
          // (or greater if reversed is true) than the pivot
          // element then get the next element from the left list        
          while(gts.stringValues[i].compareTo(spivot) < 0 || (0 == gts.stringValues[i].compareTo(spivot) && gts.ticks[i] < pivotTick)) {
            i++;
          }
          // If the current value from the right list is larger (or lower if reversed is true)
          // than the pivot element then get the next element from the right list
          while(gts.stringValues[j].compareTo(spivot) > 0 || (0 == gts.stringValues[j].compareTo(spivot) && gts.ticks[j] > pivotTick)) {
            j--;
          }
        } else {
          while(gts.stringValues[i].compareTo(spivot) > 0 || (0 == gts.stringValues[i].compareTo(spivot) && gts.ticks[i] > pivotTick)) {
            i++;
          }
          while(gts.stringValues[j].compareTo(spivot) < 0 || (0 == gts.stringValues[j].compareTo(spivot) && gts.ticks[j] < pivotTick)) {
            j--;
          }
        }
      }

      // If we have found a values in the left list which is larger then
      // the pivot element and if we have found a value in the right list
      // which is smaller then the pivot element then we exchange the
      // values.
      // As we are done we can increase i and j
      if (i <= j) {
        if (TYPE.LONG == gts.type) {
          long tmplong = gts.longValues[i];
          gts.longValues[i] = gts.longValues[j];
          gts.longValues[j] = tmplong;          
        } else if (TYPE.DOUBLE == gts.type) {
          double tmpdouble = gts.doubleValues[i];
          gts.doubleValues[i] = gts.doubleValues[j];
          gts.doubleValues[j] = tmpdouble;            
        } else if (TYPE.STRING == gts.type) { 
          String tmpstring = gts.stringValues[i];
          gts.stringValues[i] = gts.stringValues[j];
          gts.stringValues[j] = tmpstring;            
        } else if (TYPE.BOOLEAN == gts.type) {
          boolean tmpboolean = gts.booleanValues.get(i);
          gts.booleanValues.set(i, gts.booleanValues.get(j));
          gts.booleanValues.set(j, tmpboolean);
        }

        long tmplong = gts.ticks[i];
        gts.ticks[i] = gts.ticks[j];
        gts.ticks[j] = tmplong;
          
        if (null != gts.locations) {
          tmplong = gts.locations[i];
          gts.locations[i] = gts.locations[j];
          gts.locations[j] = tmplong;          
        }
          
        if (null != gts.elevations) {
          tmplong = gts.elevations[i];
          gts.elevations[i] = gts.elevations[j];
          gts.elevations[j] = tmplong;          
        }
        
        i++;
        j--;
      }
    }
    
    // Recursion
    if (low < j) {
      quicksortByValue(gts, low, j, reversed);
    }
    if (i < high) {   
      quicksortByValue(gts, i, high, reversed);
    }
  }

  /**
   * Sort GTS according to location.
   * The ticks with no locations are clustered somewhere in-between those with locations since the
   * marker for NO_LOCATION is a valid location (!)
   * 
   * @param gts
   * @param low 
   * @param high
   * @param reversed
   */
  private static final void quicksortByLocation(GeoTimeSerie gts, int low, int high, boolean reversed) { 
    
    if (0 == gts.values) {
      return;
    }
    
    if (null == gts.locations) {
      return;
    }
        
    int i = low, j = high;
    // Get the pivot element from the middle of the list
    long pivot = 0L;

    pivot = gts.locations[low + (high-low)/2];
    
    long pivotTick = gts.ticks[low + (high-low) / 2];
    
    // Divide into two lists
    while (i <= j) {
      
      if (!reversed) {
        // If the current value from the left list is smaller
        // (or greater if reversed is true) than the pivot
        // element then get the next element from the left list        
        while(gts.locations[i] < pivot || (gts.locations[i] == pivot && gts.ticks[i] < pivotTick)) {
          i++;
        }
        
        // If the current value from the right list is larger (or lower if reversed is true)
        // than the pivot element then get the next element from the right list
        while(gts.locations[j] > pivot || (gts.locations[j] == pivot && gts.ticks[j] > pivotTick)) {
          j--;
        }
      } else {
        while(gts.locations[i] > pivot || (gts.locations[i] == pivot && gts.ticks[i] > pivotTick)) {
          i++;
        }
        while(gts.locations[j] < pivot || (gts.locations[j] == pivot && gts.ticks[j] < pivotTick)) {
          j--;
        }
      }
  
      // If we have found a values in the left list which is larger then
      // the pivot element and if we have found a value in the right list
      // which is smaller then the pivot element then we exchange the
      // values.
      // As we are done we can increase i and j
      if (i <= j) {
        if (TYPE.LONG == gts.type) {
          long tmplong = gts.longValues[i];
          gts.longValues[i] = gts.longValues[j];
          gts.longValues[j] = tmplong;          
        } else if (TYPE.DOUBLE == gts.type) {
          double tmpdouble = gts.doubleValues[i];
          gts.doubleValues[i] = gts.doubleValues[j];
          gts.doubleValues[j] = tmpdouble;            
        } else if (TYPE.STRING == gts.type) { 
          String tmpstring = gts.stringValues[i];
          gts.stringValues[i] = gts.stringValues[j];
          gts.stringValues[j] = tmpstring;            
        } else if (TYPE.BOOLEAN == gts.type) {
          boolean tmpboolean = gts.booleanValues.get(i);
          gts.booleanValues.set(i, gts.booleanValues.get(j));
          gts.booleanValues.set(j, tmpboolean);
        }

        long tmplong = gts.ticks[i];
        gts.ticks[i] = gts.ticks[j];
        gts.ticks[j] = tmplong;
          
        if (null != gts.locations) {
          tmplong = gts.locations[i];
          gts.locations[i] = gts.locations[j];
          gts.locations[j] = tmplong;          
        }
          
        if (null != gts.elevations) {
          tmplong = gts.elevations[i];
          gts.elevations[i] = gts.elevations[j];
          gts.elevations[j] = tmplong;          
        }
        
        i++;
        j--;
      }
    }
    
    // Recursion
    if (low < j) {
      quicksortByLocation(gts, low, j, reversed);
    }
    if (i < high) {   
      quicksortByLocation(gts, i, high, reversed);
    }
  }

  public static GeoTimeSerie locationSort(GeoTimeSerie gts) {
    quicksortByLocation(gts,0,gts.values - 1,false);
    return gts;
  }
    
  /**
   * Return the tick at a given index in a Geo Time Serie.
   * 
   * @param gts
   * @param idx
   * @return
   */
  public static long tickAtIndex(GeoTimeSerie gts, int idx) {
    if (idx >= gts.values) {
      return Long.MIN_VALUE;
    } else {
      return gts.ticks[idx];
    }
  }

  /**
   * Return a list with the ticks in a Geo Time Serie.
   *
   * @return                                                                           ]
   */
  public static List<Long> tickList(GeoTimeSerie gts) {
    List<Long> ticks = new ArrayList<Long>(gts.values);

    ticks.addAll(Arrays.asList(ArrayUtils.toObject(Arrays.copyOf(gts.ticks, gts.values))));
    return ticks;
  }
  
  public static int indexAtTick(GeoTimeSerie gts, long tick) {
    
    if (0 == gts.values) {
      return -1;
    }
    
    sort(gts, false);
    
    //
    // Attempt to locate the tick
    //
    
    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);

    if (idx < 0) {
      return -1;      
    }
    
    return idx;
  }
  
  /**
   * Return the value in a Geo Time Serie at a given timestamp.
   * 
   * The GeoTimeSerie instance will be sorted if it is not already
   * 
   * @param gts GeoTimeSerie instance from which to extract value
   * @param tick Timestamp at which to read the value
   * @return The value at 'tick' or null if none exists
   */
  public static Object valueAtTick(GeoTimeSerie gts, long tick) {
    
    if (0 == gts.values) {
      return null;
    }
    
    //
    // Force sorting in natural ordering of ticks
    //
    
    sort(gts, false);
    
    //
    // Attempt to locate the tick
    //
    
    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);

    if (idx < 0) {
      return null;
    } else {
      if (TYPE.LONG == gts.type) {
        return gts.longValues[idx];
      } else if (TYPE.DOUBLE == gts.type) {
        return gts.doubleValues[idx];
      } else if (TYPE.STRING == gts.type) {
        return gts.stringValues[idx];
      } else if (TYPE.BOOLEAN == gts.type) {
        return gts.booleanValues.get(idx);
      } else {
        return null;
      }
    }        
  }
  
  /**
   * Return the value in a GTS instance at a given index.
   * Return null if no value exists for the given index.
   * 
   * @param gts
   * @param idx
   * @return
   */
  public static Object valueAtIndex(GeoTimeSerie gts, int idx) {
    if (idx >= gts.values) {
      return null;
    }
    if (TYPE.LONG == gts.type) {
      return gts.longValues[idx];
    } else if (TYPE.DOUBLE == gts.type) {
      return gts.doubleValues[idx];
    } else if (TYPE.STRING == gts.type) {
      return gts.stringValues[idx];
    } else if (TYPE.BOOLEAN == gts.type) {
      return gts.booleanValues.get(idx);
    } else {
      return null;
    }
  }
  
  /**
   * Return the location in a Geo Time Serie at a given timestamp.
   * 
   * The GeoTimeSerie instance will be sorted if it is not already.
   * 
   * @param gts GeoTimeSerie instance from which to extract location
   * @param tick Timestamp at which to read the location
   * @return The location at 'tick' (NO_LOCATION if none set).
   */
  public static long locationAtTick(GeoTimeSerie gts, long tick) {
    
    if (null == gts.locations) {
      return GeoTimeSerie.NO_LOCATION;
    }
    
    sort(gts, false);

    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);
    
    if (idx < 0) {
      return GeoTimeSerie.NO_LOCATION;
    } else {
      return gts.locations[idx];
    }
  }

  /**
   * Return the location in a GeoTimeSerie at a given index
   * 
   * @param gts
   * @param idx
   * @return
   */
  public static long locationAtIndex(GeoTimeSerie gts, int idx) {
    if (null == gts.locations || idx >= gts.values) {
      return GeoTimeSerie.NO_LOCATION;
    } else {
      return gts.locations[idx];
    }    
  }

  /**
   * Set the location at a specific index in the GTS
   * 
   * @param gts
   * @param idx
   * @param location
   */
  public static void setLocationAtIndex(GeoTimeSerie gts, int idx, long location) {
    if (idx >= gts.values) {
      return;
    }
    
    if (null != gts.locations) {
      gts.locations[idx] = location;
    } else {
      if (GeoTimeSerie.NO_LOCATION != location) {
        gts.locations = new long[gts.values];
        Arrays.fill(gts.locations, GeoTimeSerie.NO_LOCATION);
        gts.locations[idx] = location;
      }
    }
  }
  
  /**
   * Return the elevation in a Geo Time Serie at a given timestamp.
   * 
   * The GeoTimeSerie instance will be sorted if it is not already.
   * 
   * @param gts GeoTimeSerie instance from which to extract elevation
   * @param tick Timestamp at which to read the elevation
   * @return The elevation at 'tick' (NO_ELEVATION if none set).
   */
  public static long elevationAtTick(GeoTimeSerie gts, long tick) {
    
    if (null == gts.elevations) {
      return GeoTimeSerie.NO_ELEVATION;
    }
    
    sort(gts, false);

    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);
    
    if (idx < 0) {
      return GeoTimeSerie.NO_ELEVATION;
    } else {
      return gts.elevations[idx];
    }
  }
  /**
   * Set the elevation at a specific index in the GTS
   * 
   * @param gts
   * @param idx
   * @param location
   */
  public static void setElevationAtIndex(GeoTimeSerie gts, int idx, long elevation) {
    if (idx >= gts.values) {
      return;
    }
    
    if (null != gts.elevations) {
      gts.elevations[idx] = elevation;
    } else {
      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        gts.elevations = new long[gts.values];
        Arrays.fill(gts.elevations, GeoTimeSerie.NO_ELEVATION);
        gts.elevations[idx] = elevation;
      }
    }
  }
  
  /**
   * Return the elevation in a GeoTimeSerie at a given index
   * 
   * @param gts
   * @param idx
   * @return
   */
  public static long elevationAtIndex(GeoTimeSerie gts, int idx) {
    if (null == gts.elevations || idx >= gts.values) {
      return GeoTimeSerie.NO_ELEVATION;
    } else {
      return gts.elevations[idx];
    }    
  }
  
  /**
   * Remove a datapoint from a GTS.
   * 
   * @param gts The GTS to alter
   * @param timestamp The timestamp at which to remove the value
   * @param all Boolean indicating whether or not we should remove all occurrences or simply the first one found
   */
  public static void removeValue(GeoTimeSerie gts, long timestamp, boolean all) {    
    GeoTimeSerie altered = gts.cloneEmpty(gts.values);
    
    int todelete = Integer.MAX_VALUE;
    
    if (all) {
      todelete = 1;
    }
    
    for (int i = 0; i < gts.values; i++) {
      if (todelete > 1 && timestamp == gts.ticks[i]) {
        todelete--;
        continue;
      }
      GTSHelper.setValue(altered, gts.ticks[i], GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
    }
  }

  /**
   * Add a measurement at the given timestamp/location/elevation
   * 
   * FIXME(hbs): If performance is bad due to autoboxing, we can always split addValue by type
   *             and have 4 methods instead of 1.
   *             
   * @param gts GeoTimeSerie instance to which the measurement must be added
   * @param timestamp Timestamp in microseconds of the measurement
   * @param geoxppoint Location of measurement as a GeoXPPoint
   * @param elevation Elevation of measurement in millimeters
   * @param value Value of measurement
   * @param overwrite Flag indicating whether or not to overwrite a previous measurement done at the same timestamp
   * 
   * @return The number of values in the GeoTimeSerie, including the one that was just added
   */
  
  public static final int setValue(GeoTimeSerie gts, long timestamp, long geoxppoint, long elevation, Object value, boolean overwrite) {
    
    //
    // Ignore nulls
    //
    
    if (null == value) {
      return gts.values;
    }
      
    //
    // If 'overwrite' is true, check if 'timestamp' is already in 'ticks'
    // If so, record new value there.
    //

    int idx = gts.values;
    
    if (overwrite) {
      for (int i = 0; i < gts.values; i++) {
        if (timestamp == gts.ticks[i]) {
          idx = i;
          break;
        }
      }
    }
        
    //
    // Provision memory allocation for the new value.
    //
    
    if (gts.values == idx) {
      // Reset 'sorted' flag as we add a value
      gts.sorted = false;
      if (TYPE.UNDEFINED == gts.type || null == gts.ticks || gts.values >= gts.ticks.length || (null == gts.locations && GeoTimeSerie.NO_LOCATION != geoxppoint) || (null == gts.elevations && GeoTimeSerie.NO_ELEVATION != elevation)) {
        provision(gts, value, geoxppoint, elevation);
      }
    } else if ((null == gts.locations && GeoTimeSerie.NO_LOCATION != geoxppoint) || (null == gts.elevations && GeoTimeSerie.NO_ELEVATION != elevation)) {
      provision(gts, value, geoxppoint, elevation);      
    }
    
    //
    // Record timestamp, location, elevation
    //
  
    gts.ticks[idx] = timestamp;
    
    if (null != gts.locations) {
      gts.locations[idx] = geoxppoint;
    }
    
    if (null != gts.elevations) {
      gts.elevations[idx] = elevation;
    }
    
    //
    // Record value, doing conversions if need be
    //
    
    if (value instanceof Boolean) {
      if (TYPE.LONG == gts.type) {
        gts.longValues[idx] = ((Boolean) value).booleanValue() ? 1L : 0L; 
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues[idx] = ((Boolean) value).booleanValue() ? 1.0D : 0.0D; 
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues[idx] = ((Boolean) value).booleanValue() ? "T" : "F"; 
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, ((Boolean) value).booleanValue());
      }      
    } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
      if (TYPE.LONG == gts.type) {
        gts.longValues[idx] = ((Number) value).longValue(); 
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues[idx] = ((Number) value).doubleValue();
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues[idx] = ((Number) value).toString();
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, 0L != ((Number) value).longValue());
      }
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      if (TYPE.LONG == gts.type) {
        gts.longValues[idx] = ((Number) value).longValue(); 
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues[idx] = ((Number) value).doubleValue();
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues[idx] = value.toString();
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, 0.0D != ((Number) value).doubleValue());
      }      
    } else if (value instanceof String) {
      if (TYPE.LONG == gts.type) {
        try {
          //gts.longValues[idx] = Long.valueOf((String) value);
          gts.longValues[idx] = Long.parseLong((String) value);
        } catch (NumberFormatException nfe) {
          //
          // Attempt to parse a double
          //
          try {
            gts.longValues[idx] = (long) Double.parseDouble((String) value);
          } catch (NumberFormatException nfe2) {
            gts.longValues[idx] = 0L;            
          }          
        }
      } else if (TYPE.DOUBLE == gts.type) {
        try {
          //gts.doubleValues[idx] = Double.valueOf((String) value);
          gts.doubleValues[idx] = Double.parseDouble((String) value);
        } catch (NumberFormatException nfe) {
          try {
            gts.doubleValues[idx] = (double) Long.parseLong((String) value);
          } catch (NumberFormatException nfe2) {
            gts.doubleValues[idx] = 0.0D;
          }
        }
      } else if (TYPE.STRING == gts.type) {
        // Using intern is really CPU intensive
        gts.stringValues[idx] = (String) value; //.toString().intern();
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, null != value && !"".equals(value));
      }      
    } else {
      //
      // Ignore other types
      //
      
      return gts.values;
    }
    
    //
    // Increment number of stored values if we did not overwrite a previous one
    //
    
    if (gts.values == idx) {
      gts.values++;
    }

    return gts.values;
  }

  public static final int setValue(GeoTimeSerie gts, long timestamp, long geoxppoint, Object value) {
    return setValue(gts, timestamp, geoxppoint, GeoTimeSerie.NO_ELEVATION, value, false);
  }
  
  public static final int setValue(GeoTimeSerie gts, long timestamp, Object value) {
    return setValue(gts, timestamp, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, value, false);
  }
  
  /**
   * Allocate memory so we can add one value to the Geo Time Serie.
   * 
   * @param value The value that will be added, it is just used so we can allocate the correct container for the type.
   */
  private static final void provision(GeoTimeSerie gts, Object value, long location, long elevation) {
    //
    // Nothing to do if the ticks array is not full yet.
    //
    if (TYPE.UNDEFINED != gts.type && gts.values < gts.ticks.length) {
      if (GeoTimeSerie.NO_LOCATION == location && GeoTimeSerie.NO_ELEVATION == elevation) {
        return;
      }
      
      if (null == gts.locations && GeoTimeSerie.NO_LOCATION != location) {
        gts.locations = new long[gts.ticks.length];
        Arrays.fill(gts.locations, GeoTimeSerie.NO_LOCATION);
      }
      
      if (null == gts.elevations && GeoTimeSerie.NO_ELEVATION != elevation) {
        gts.elevations = new long[gts.ticks.length];
        Arrays.fill(gts.elevations, GeoTimeSerie.NO_ELEVATION);
      }
      
      return;
    } else if (TYPE.UNDEFINED != gts.type) {
      //
      // We need to grow 'ticks', 'locations', 'elevations' and associated value array.
      //
      
      int newlen = gts.ticks.length + (int) Math.min(GeoTimeSerie.MAX_ARRAY_GROWTH, Math.max(GeoTimeSerie.MIN_ARRAY_GROWTH, gts.ticks.length * GeoTimeSerie.ARRAY_GROWTH_FACTOR));
      
      //if (newlen > MAX_VALUES) {
      //  throw new RuntimeException("Geo time serie would exceed maximum number of values set to " + MAX_VALUES);
      //}

      if (newlen < gts.sizehint) {
        newlen = gts.sizehint;
      }
      
      gts.ticks = Arrays.copyOf(gts.ticks, newlen);
      if (null != gts.locations || GeoTimeSerie.NO_LOCATION != location) {
        if (null == gts.locations) {
          gts.locations = new long[gts.ticks.length];
          // Fill all values with NO_LOCATION since we are creating the array and thus
          // must consider all previous locations were undefined
          Arrays.fill(gts.locations, GeoTimeSerie.NO_LOCATION);
        } else {
          gts.locations = Arrays.copyOf(gts.locations, gts.ticks.length);
        }
      }
      if (null != gts.elevations || GeoTimeSerie.NO_ELEVATION != elevation) {
        if (null == gts.elevations) {
          gts.elevations = new long[gts.ticks.length];
          // Fill the newly allocated array with NO_ELEVATION since we must consider
          // all previous elevations were undefined
          Arrays.fill(gts.elevations, GeoTimeSerie.NO_ELEVATION);
        } else {
          gts.elevations = Arrays.copyOf(gts.elevations, gts.ticks.length);
        }
      }
      
      // BitSets grow automatically...
      if (TYPE.LONG == gts.type) {
        gts.longValues = Arrays.copyOf(gts.longValues, gts.ticks.length);
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues = Arrays.copyOf(gts.doubleValues, gts.ticks.length);
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues = Arrays.copyOf(gts.stringValues, gts.ticks.length);
      }
    } else if (TYPE.UNDEFINED == gts.type) {
      if (null == gts.ticks) {
        gts.ticks = new long[gts.sizehint > 0 ? gts.sizehint : GeoTimeSerie.MIN_ARRAY_GROWTH];
      }

      // Nullify location if no location is set (since the GTS is empty)
      if (GeoTimeSerie.NO_LOCATION == location) {
        gts.locations = null;
      } else if (null == gts.locations || gts.locations.length < gts.ticks.length) {
        gts.locations = new long[gts.ticks.length];
      }
            
      // Nullify elevation if no elevation is set (since the GTS is empty)
      if (GeoTimeSerie.NO_ELEVATION == elevation) {
        gts.elevations = null;
      } else if (null == gts.elevations || gts.elevations.length < gts.ticks.length) {
        gts.elevations = new long[gts.ticks.length];
      }
      
      if (value instanceof Boolean) {
        gts.type = TYPE.BOOLEAN;
        if (null == gts.booleanValues || gts.booleanValues.size() < gts.ticks.length) {
          gts.booleanValues = new BitSet(gts.ticks.length);
        }
      } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
        gts.type = TYPE.LONG;
        if (null == gts.longValues || gts.longValues.length < gts.ticks.length) {
          gts.longValues = new long[gts.ticks.length];
        }
      } else if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
        gts.type = TYPE.DOUBLE;
        if (null == gts.doubleValues || gts.doubleValues.length < gts.ticks.length) {
          gts.doubleValues = new double[gts.ticks.length];
        }
      } else if (value instanceof String) {
        gts.type = TYPE.STRING;
        if (null == gts.stringValues || gts.stringValues.length < gts.ticks.length) {
          gts.stringValues = new String[gts.ticks.length];
        }
      } else {
        //
        // Default type is boolean, this is so people will rapidly notice
        // this is not what they were expecting...
        //
        gts.type = TYPE.BOOLEAN;
        gts.booleanValues = new BitSet(gts.ticks.length);
      }
    }    
  }
  
  /**
   * Return a new GeoTimeSerie instance containing only the value of 'gts'
   * which fall between 'timestamp' (inclusive) and 'timestamp' - 'span' (exclusive)
   * 
   * The resulting GTS instance will be sorted.
   * 
   * @param gts GeoTimeSerie from which to extract values.
   * @param starttimestamp Oldest timestamp to consider (in microseconds)
   * @param stoptimestamp Most recent timestamp to consider (in microseconds)
   * @param overwrite Should we overwrite measurements which occur at the same timestamp to only keep the last one added.
   * @param copyLabels If true, labels will be copied from the original GTS to the subserie.
   * 
   * @return The computed sub Geo Time Serie
   */
  public static final GeoTimeSerie subSerie(GeoTimeSerie gts, long starttimestamp, long stoptimestamp, boolean overwrite, boolean copyLabels, GeoTimeSerie subgts) {
    // FIXME(hbs): should a subserie of a bucketized GTS be also bucketized? With possible non existant values. This would impact Mappers/Bucketizers
    
    //
    // Create sub GTS
    //
    
    // The size hint impacts performance, choose it wisely...
    if (null == subgts) {
      subgts = new GeoTimeSerie(128);
      //
      // Copy name and labels
      //
      
      subgts.setName(gts.getName());
      
      if (copyLabels) {
        subgts.setLabels(gts.getLabels());
      }
    } else {
      GTSHelper.reset(subgts);
    }
    
    if (null == gts.ticks || 0 == gts.values) {
      return subgts;
    }

    //
    // Sort GTS so ticks are ordered
    //
    
    GTSHelper.sort(gts);
        
    //
    // Determine index to start at
    //
    
    int lastidx = Arrays.binarySearch(gts.ticks, 0, gts.values, stoptimestamp);
    
    //
    // The upper timestamp is less than the first tick, so subserie is necessarly empty
    //
    
    if (-1 == lastidx) {
      return subgts;
    } else if (lastidx < 0) {
      lastidx =  -lastidx - 1;
      if (lastidx >= gts.values) {
        lastidx = gts.values - 1;
      }
    }
    
    int firstidx = Arrays.binarySearch(gts.ticks, 0, lastidx + 1, starttimestamp);
    
    if (firstidx < 0) {
      firstidx = -firstidx - 1;
    }
    if (firstidx >= gts.values) {
      return subgts;
    }
    
    //
    // Extract values/locations/elevations that lie in the requested interval
    //
    
    for (int i = firstidx; i <= lastidx; i++) {
      if (gts.ticks[i] >= starttimestamp && gts.ticks[i] <= stoptimestamp) {
        setValue(subgts, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), overwrite);
      }
    }

    return subgts;
  }

  public static final GeoTimeSerie subSerie(GeoTimeSerie gts, long starttimestamp, long stoptimestamp, boolean overwrite) {
    return subSerie(gts, starttimestamp, stoptimestamp, overwrite, true, null);
  }
  
  /**
   * Return a new GeoTimeSerie instance containing only the values of 'gts'
   * that fall under timestamps that are bound by the same modulo class.
   * 
   * The resulting GTS instance will be sorted and bucketized.
   * 
   * @param gts GeoTimeSerie from which to extract values. It must be bucketized.
   * @param lastbucket Most recent timestamp to consider (in microseconds)
   * @param buckets_per_period Number of buckets of the input gts that sum up to a bucket for the sub cycle gts
   * @param overwrite Should we overwrite measurements which occur at the same timestamp to only keep the last one added.
   * 
   * @return The computed sub cycle Geo Time Serie
   */
  public static final GeoTimeSerie subCycleSerie(GeoTimeSerie gts, long lastbucket, int buckets_per_period, boolean overwrite, GeoTimeSerie subgts) throws WarpScriptException {
    if (!isBucketized(gts)) {
      throw new WarpScriptException("GTS must be bucketized");
    }
    
    if (0 != (gts.lastbucket - lastbucket) % gts.bucketspan) {
      throw new WarpScriptException("lasbucket parameter of subCycleSerie method must fall on an actual bucket of the gts input");
    }
    
    //
    // Create sub GTS
    //
    
    // The size hint impacts performance, choose it wisely...
    if (null == subgts) {
      subgts = new GeoTimeSerie(lastbucket, (gts.bucketcount - (int) ((gts.lastbucket - lastbucket) / gts.bucketspan) - 1) / buckets_per_period + 1, gts.bucketspan * buckets_per_period, (int) Math.max(1.4 * gts.bucketcount, gts.sizehint) / buckets_per_period);
    } else {
      subgts.values = 0;
      subgts.type = TYPE.UNDEFINED;
      
      subgts.lastbucket = lastbucket;
      subgts.bucketcount = (gts.bucketcount - (int) ((gts.lastbucket - lastbucket) / gts.bucketspan) - 1) / buckets_per_period + 1;
      subgts.bucketspan = gts.bucketspan * buckets_per_period;      
    }
    
    if (null == gts.ticks || 0 == gts.values) {
      return subgts;
    }
    
    //
    // For each tick, search if tick is in gts, then copy it to subgts, else noop
    //    
    
    Iterator<Long> iter = tickIterator(subgts, true);
    long tick;
    sort(gts);
    int i = gts.values;
    while (iter.hasNext()) {
      
      tick = iter.next();
      i = Arrays.binarySearch(gts.ticks, 0, i, tick);
      
      if (i >= 0) {
        setValue(subgts, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), overwrite);
      }
    }

    return subgts;
  }
  
  public static final GeoTimeSerie subCycleSerie(GeoTimeSerie gts, long lastbucket, int buckets_per_period, boolean overwrite) throws WarpScriptException {
    return subCycleSerie(gts, lastbucket, buckets_per_period, true, null);
  }
  
  /**
   * Converts a Geo Time Serie into a bucketized version.
   * Bucketizing means aggregating values (with associated location and elevation) that lie
   * within a given interval into a single one. Intervals considered span a given
   * width called the bucketspan.
   * 
   * @param gts Geo Time Serie to bucketize
   * @param bucketspan Width of bucket (time interval) in microseconds
   * @param bucketcount Number of buckets to use
   * @param lastbucket Timestamp of the end of the last bucket of the resulting GTS. If 0, 'lastbucket' will be set to the last timestamp of 'gts'.
   * @param aggregator Function used to aggregate values/locations/elevations
   * @return A bucketized version of the GTS.
   */
  public static final GeoTimeSerie bucketize(GeoTimeSerie gts, long bucketspan, int bucketcount, long lastbucket, WarpScriptBucketizerFunction aggregator, long maxbuckets) throws WarpScriptException {
    return bucketize(gts, bucketspan, bucketcount, lastbucket, aggregator, maxbuckets, null);
  }
  
  public static final GeoTimeSerie bucketize(GeoTimeSerie gts, long bucketspan, int bucketcount, long lastbucket, Object aggregator, long maxbuckets, WarpScriptStack stack) throws WarpScriptException {

    //
    // If lastbucket is 0, compute it from the last timestamp
    // in gts and make it congruent to 0 modulus 'bucketspan'
    //
    
    long lasttick = GTSHelper.lasttick(gts);
    long firsttick = GTSHelper.firsttick(gts);

    boolean zeroLastBucket = 0 == lastbucket;
    boolean zeroBucketcount = 0 == bucketcount;
    
    //
    // If lastbucket AND bucketcount AND bucketspan are 0
    //
    
    if (0 == lastbucket) {
      lastbucket = lasttick;
    }

    //
    // If bucketspan is 0 but bucketcount is set, compute bucketspan so 'bucketcount' buckets
    // cover the complete set of values from firsttick to lastbucket
    //
    
    if (0 == bucketspan && 0 != bucketcount) {
      if (lastbucket >= firsttick) {
        long delta = lastbucket - firsttick + 1;
        bucketspan = delta / bucketcount;

        //
        // Increase bucketspan by 1 so we cover the whole timespan
        //
        
        if (0 == bucketspan || (delta % bucketspan) != 0) {
          bucketspan++;
        }
      }
    } else if (0 == bucketspan && 0 == bucketcount) {
      throw new WarpScriptException("One of bucketspan or bucketcount must be different from zero.");
    }
    
    //
    // If bucketcount is 0, compute it so the bucketized GTS covers all ticks
    //    
    
    if (0 == bucketcount) {      
      if (lastbucket >= firsttick) {
        long delta = lastbucket - firsttick;
        
        if (delta < bucketspan) {
          bucketcount = 1;
        } else {
          bucketcount = 1 + (int) (delta / bucketspan);
        }
      }
    }

    //
    // Now that bucketspan is computed, adjust lastbucket if the passed value was '0' so
    // it lies on a bucketspan boundary.
    //
    
    if (zeroLastBucket && zeroBucketcount) {     
      // Make sure lastbucket falls on a bucketspan boundary (only if bucketspan was specified)
      if (0 != lastbucket % bucketspan) {
        lastbucket = lastbucket - (lastbucket % bucketspan) + bucketspan;
        if (lastbucket - bucketcount * bucketspan >= firsttick) {
          bucketcount++;
        }
      }
    }
    
    if (bucketcount < 0 || bucketcount > maxbuckets) {
      throw new WarpScriptException("Bucket count (" + bucketcount + ") would exceed maximum value of " + maxbuckets);
    }

    if (0 == bucketspan) {
      throw new WarpScriptException("Undefined bucket span, check your GTS timestamps.");
    }
    
    //
    // Create the target Geo Time Serie (bucketized)
    //
    
    // Determine sizing hint. We evaluate the number of buckets that will be filled.
    
    int hint = Math.min(gts.values, (int) ((lasttick - firsttick) / bucketspan));
    
    GeoTimeSerie bucketized = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, hint);
    
    //
    // Copy name, labels and attributes
    //
    
    bucketized.setMetadata(new Metadata(gts.getMetadata()));

    Map<String,String> labels = gts.getLabels();
    
    //
    // Loop on all buckets
    //

    //
    // We can't skip buckets which are before the first tick or after the last one
    // because the bucketizer function might set a default value when it encounters an
    // empty sub serie
    //
      
    // Allocate a stable GTS instance which we will reuse when calling subserie
    GeoTimeSerie subgts = null;
    
    for (int i = 0; i < bucketcount; i++) {
      
      long bucketend = lastbucket - i * bucketspan;
      
      //
      // Extract GTS containing the values that fall in the bucket
      // Keep multiple values that fall on the same timestamp, the
      // aggregator functions will deal with them.
      //
      
      subgts = subSerie(gts, bucketend - bucketspan + 1, bucketend, false, false, subgts);

      if (0 == subgts.values) {
        continue;
      }
      
      Object[] aggregated = null;
      
      if (null != stack) {
        if (!(aggregator instanceof Macro)) {
          throw new WarpScriptException("Expected a macro as bucketizer.");
        }
        
        subgts.safeSetMetadata(bucketized.getMetadata());
        stack.push(subgts);
        stack.exec((Macro) aggregator);
        
        Object res = stack.peek();
        
        if (res instanceof List) {
          aggregated = MACROMAPPER.listToObjects((List<Object>) stack.pop());
        } else {
          aggregated = MACROMAPPER.stackToObjects(stack);
        }                
      } else {
        if (!(aggregator instanceof WarpScriptBucketizerFunction)) {
          throw new WarpScriptException("Invalid bucketizer function.");
        }
        //
        // Call the aggregation functions on this sub serie and add the resulting value
        //
        
        //
        // Aggregator functions have 8 parameters (so mappers or reducers can be used as aggregators)
        //
        // bucket timestamp: end timestamp of the bucket we're currently computing a value for
        // names: array of GTS names
        // labels: array of GTS labels
        // ticks: array of ticks being aggregated
        // locations: array of locations being aggregated
        // elevations: array of elevations being aggregated
        // values: array of values being aggregated
        // bucket span: width (in microseconds) of bucket
        //
        
        Object[] parms = new Object[8];

        int idx = 0;
        parms[idx++] = bucketend;
        parms[idx] = new String[1];
        ((String[]) parms[idx++])[0] = bucketized.getName();
        parms[idx] = new Map[1];
        ((Map[]) parms[idx++])[0] = labels;
        parms[idx++] = Arrays.copyOf(subgts.ticks, subgts.values);
        if (null != subgts.locations) {
          parms[idx++] = Arrays.copyOf(subgts.locations, subgts.values);
        } else {
          parms[idx++] = new long[subgts.values];
          Arrays.fill((long[]) parms[idx - 1], GeoTimeSerie.NO_LOCATION);
        }
        if (null != subgts.elevations) {
          parms[idx++] = Arrays.copyOf(subgts.elevations, subgts.values);
        } else {
          parms[idx++] = new long[subgts.values];
          Arrays.fill((long[]) parms[idx - 1], GeoTimeSerie.NO_ELEVATION);
        }
        parms[idx++] = new Object[subgts.values];
        parms[idx++] = new long[] { 0, -bucketspan, bucketend - bucketspan, bucketend };
        
        for (int j = 0; j < subgts.values; j++) {
          ((Object[]) parms[6])[j] = valueAtIndex(subgts, j);
        }

        aggregated = (Object[]) ((WarpScriptBucketizerFunction) aggregator).apply(parms);        
      }

      //
      // Only set value if it was non null
      //
      
      if (null != aggregated[3]) {
        setValue(bucketized, bucketend, (long) aggregated[1], (long) aggregated[2], aggregated[3], false);
      }
    }
    
    GTSHelper.shrink(bucketized);
    return bucketized;
  }

  public static void unbucketize(GeoTimeSerie gts) {
    gts.bucketcount = 0;
    gts.bucketspan = 0L;
    gts.lastbucket = 0L;
  }
  
  /**
   * Parses a string representation of a measurement and return a single valued GTS
   * 
   * @param str String representation to parse
   * @return The resulting data in a GTSEncoder. The resulting encoder will not have classId/labelsId set.
   * 
   * @throws InvalidFormatException if a parsing error occurred
   */
  private static final Pattern MEASUREMENT_RE = Pattern.compile("^([0-9]+)?/(([0-9.-]+):([0-9.-]+))?/([0-9-]+)? +([^ ]+)\\{([^\\}]*)\\} +(.+)$");
  
  public static GTSEncoder parse_regexp(GTSEncoder encoder, String str, Map<String,String> extraLabels) throws ParseException, IOException {
    Matcher matcher = MEASUREMENT_RE.matcher(str);
    
    if (!matcher.matches()) {
      throw new ParseException(str, 0);
    }
    
    //
    // Check name
    //
    
    String name = matcher.group(6);
    
    if (name.contains("%")) {
      try {      
        name = URLDecoder.decode(name, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        // Can't happen, we're using UTF-8
      }      
    }
    
    //
    // Parse labels
    //
    
    Map<String,String> labels = parseLabels(matcher.group(7));
    
    //
    // Add any provided extra labels
    //
    
    if (null != extraLabels) {
      labels.putAll(extraLabels);
    }
    
    //
    // Extract timestamp, optional location and elevation
    //
    
    long timestamp;
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    try {
      
      if (null != matcher.group(1)) {
        //timestamp = Long.valueOf(matcher.group(1));
        timestamp = Long.parseLong(matcher.group(1));
      } else {
        // No timestamp provided, use 'now'
        timestamp = TimeSource.getTime();;
      }
      
      if (null != matcher.group(2)) {
        //location = GeoXPLib.toGeoXPPoint(Double.valueOf(matcher.group(3)), Double.valueOf(matcher.group(4)));
        location = GeoXPLib.toGeoXPPoint(Double.parseDouble(matcher.group(3)), Double.parseDouble(matcher.group(4)));
      }
      
      if (null != matcher.group(5)) {
        //elevation = Long.valueOf(matcher.group(5));
        elevation = Long.parseLong(matcher.group(5));
      }      
    } catch (NumberFormatException nfe) {
      throw new ParseException("", 0);
    }
    
    //
    // Extract value
    //
    
    
    String valuestr = matcher.group(8);

    Object value = parseValue_regexp(valuestr);
      
    if (null == value) {
      throw new ParseException("Unable to parse value '" + valuestr + "'", 0);
    }
    
    // Allocate a new Encoder if need be, with a base timestamp of 0L.
    if (null == encoder || !name.equals(encoder.getName()) || !labels.equals(encoder.getLabels())) {
      encoder = new GTSEncoder(0L);
      encoder.setName(name);
      encoder.setLabels(labels);
    }
    
    encoder.addValue(timestamp, location, elevation, value);
    
    /*
    GeoTimeSerie gts = new GeoTimeSerie(1);
    
    gts.setName(name);
    gts.setLabels(labels);
    GTSHelper.setValue(gts, timestamp, location, elevation, value, false);
        
    return gts;
    */
    return encoder;
  }

  private static final Type GSON_MAP_TYPE = new TypeToken<Map<String,Object>>() {}.getType();
  
  private static JsonParserFactory jpf = new JsonParserFactory();
  public static GTSEncoder parseJSON(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now) throws IOException, ParseException {
    
    JsonParser parser = jpf.createFastParser();
    
    //Gson gson = new Gson();
    //Map<String,Object> o = gson.fromJson(str, GSON_MAP_TYPE);
    Map<String,Object> o = (Map<String,Object>) parser.parse(str);
    
    String name = (String) o.get("c");
    Map<String,String> labels = (Map<String,String>) o.get("l");
    
    //
    // Add any provided extra labels
    //
    
    if (null != extraLabels) {
      labels.putAll(extraLabels);
    
      //
      // Remove labels with null values
      //
      // FIXME(hbs): may be removed when dummy tokens have disappeared
      //
      
      if (extraLabels.containsValue(null)) {
        Set<Entry<String,String>> entries = extraLabels.entrySet();
        
        while(labels.containsValue(null)) {
          for (Entry<String,String> entry: entries) {
            if (null == entry.getValue()) {
              labels.remove(entry.getKey());
            }
          }
        }        
      }
    }

    Object ots = o.get("t");
    
    long ts = (null != ots ? ((Number) ots).longValue() : (null != now ? (long) now : TimeSource.getTime()));
    
    long location = GeoTimeSerie.NO_LOCATION;
    
    if (o.containsKey("lat") && o.containsKey("lon")) {
      double lat = (double) o.get("lat");
      double lon = (double) o.get("lon");
      
      location = GeoXPLib.toGeoXPPoint(lat, lon);
    }
    
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (o.containsKey("elev")) {
      elevation = ((Number) o.get("elev")).longValue();
    }
    
    Object v = o.get("v");
    
    // Allocate a new Encoder if need be, with a base timestamp of 0L.
    if (null == encoder || !name.equals(encoder.getName()) || !labels.equals(encoder.getLabels())) {
      encoder = new GTSEncoder(0L);
      encoder.setName(name);
      encoder.setLabels(labels);
    }
    
    if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte || v instanceof BigInteger) {
      encoder.addValue(ts, location, elevation, ((Number) v).longValue());
    } else if (v instanceof Double || v instanceof Float) {
      encoder.addValue(ts, location, elevation, ((Number) v).doubleValue());      
    } else if (v instanceof BigDecimal) {
      encoder.addValue(ts, location, elevation, v);
    } else if (v instanceof Boolean || v instanceof String) {
      encoder.addValue(ts, location, elevation, v);
    } else {
      throw new ParseException("Invalid value.", 0);
    }
    
    return encoder;
  }
  
  private static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, null);
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, now, Long.MAX_VALUE, false);
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now, long maxValueSize) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, now, maxValueSize, false);    
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now, long maxValueSize, boolean parseAttributes) throws ParseException, IOException {

    int idx = 0;
    
    int tsoffset = 0;
    
    if ('=' == str.charAt(0)) {
      if (null == encoder) {
        throw new ParseException("Invalid continuation.", 0);
      }
      tsoffset = 1;
    }
    
    //idx = str.indexOf("/");
    idx = UnsafeString.indexOf(str, '/');

    if (-1 == idx){
      throw new ParseException("Missing timestamp separator.", idx);
    }
    
    long timestamp;
    
    if (tsoffset == idx) {
      // No timestamp provided, use 'now'
      timestamp = null != now ? (long) now : TimeSource.getTime();
    } else {
      if ('T' == str.charAt(tsoffset)) {
        // Support T-XXX to record timestamps which are relative to 'now', useful for
        // devices with no time reference but only relative timestamps
        //timestamp = (null != now ? (long) now : TimeSource.getTime()) + Long.valueOf(str.substring(1 + tsoffset, idx));
        timestamp = (null != now ? (long) now : TimeSource.getTime()) + Long.parseLong(str.substring(1 + tsoffset, idx));        
      } else {
        //timestamp = Long.valueOf(str.substring(tsoffset,  idx));
        timestamp = Long.parseLong(str.substring(tsoffset,  idx));
      }
    }

    // Advance past the '/'
    idx++;
    
    //int idx2 = str.indexOf("/", idx);
    int idx2 = UnsafeString.indexOf(str, '/', idx);
    
    if (-1 == idx2){
      throw new ParseException("Missing location/elevation separator.", idx);
    }

    long location = GeoTimeSerie.NO_LOCATION;

    if (idx != idx2) {
      // We have a location (lat:lon)
      String latlon = str.substring(idx, idx2);
      // Advance past the second '/'    
      idx = idx2 + 1;
      //idx2 = latlon.indexOf(":");
      idx2 = UnsafeString.indexOf(latlon, ':');
            
      //location = GeoXPLib.toGeoXPPoint(Double.valueOf(latlon.substring(0, idx2)), Double.valueOf(latlon.substring(idx2 + 1)));
      location = GeoXPLib.toGeoXPPoint(Double.parseDouble(latlon.substring(0, idx2)), Double.parseDouble(latlon.substring(idx2 + 1)));
    } else {
      // Advance past the second '/'    
      idx = idx2 + 1;
    }
    
    //idx2 = str.indexOf(" ", idx);
    idx2 = UnsafeString.indexOf(str, ' ', idx);
    
    if (-1 == idx2){
      throw new ParseException(str, idx);
    }

    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (idx != idx2) {
      // We have an elevation
      //elevation = Long.valueOf(str.substring(idx, idx2));
      elevation = Long.parseLong(str.substring(idx, idx2));
    }

    // Advance past the ' '    
    idx = idx2 + 1;
    
    while (idx < str.length() && str.charAt(idx) == ' ') {
      idx++;
    }

    // If line started with '=', assume there is no class+labels component
    if (tsoffset > 0) {
      idx2 = -1;
    } else {
      //idx2 = str.indexOf("{", idx);
      idx2 = UnsafeString.indexOf(str, '{', idx);      
    }

    String name = null;
    Map<String,String> labels = null;
    Map<String,String>  attributes = null;
    
    boolean reuseLabels = false;
    
    if (-1 == idx2){
      // If we are over the end of the string, we're missing a value
      if (idx >= str.length()) {
        throw new ParseException("Missing value", idx);
      }
      // No class+labels, assume same class+labels as those in encoder, except
      // if encoder is null in which case we throw a parse exception
      if (null == encoder) {
        throw new ParseException(str, idx);
      }
      name = encoder.getMetadata().getName();
      labels = encoder.getMetadata().getLabels();
      reuseLabels = true;
    } else {
      name = str.substring(idx, idx2);
      
      //if (name.contains("%")) {
      if (-1 != UnsafeString.indexOf(name, '%')) {
        try {      
          name = URLDecoder.decode(name, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
          // Can't happen, we're using UTF-8
        }      
      }

      // Advance past the '{'
      idx = idx2 + 1;
      
      //idx2 = str.indexOf("}", idx);
      idx2 = UnsafeString.indexOf(str, '}', idx);
      
      if (-1 == idx2){
        throw new ParseException(str, idx);
      }
      
      //
      // Parse labels
      //
      
      labels = parseLabels(null != extraLabels ? extraLabels.size() : 0, str.substring(idx, idx2));           
      
      //
      // FIXME(hbs): parse attributes????
      //
      
      // Advance past the '}' and over spaces
      
      idx = idx2 + 1;

      // FIXME(hbs): should we skip over attributes if they are present?
      if (idx < str.length() && str.charAt(idx) == '{') {        
        idx++;
        int attrstart = idx;
        while(idx < str.length() && str.charAt(idx) != '}') {
          idx++;
        }
        if (parseAttributes) {
          if (idx >= str.length()) {
            throw new ParseException("Missing attributes.", idx2);
          }
          attributes = parseLabels(str.substring(attrstart, idx));
        }
        idx++;
      }
      
      while (idx < str.length() && str.charAt(idx) == ' ') {
        idx++;
      }
      
      if (idx >= str.length()) {
        throw new ParseException("Missing value.", idx2);
      }
    }

    //
    // Add any provided extra labels
    //
    // WARNING(hbs): as we check reuseLabels, note that extraLabels won't be pushed onto the GTS labels
    // if reuseLabels is 'true', this means that if you call parse on a continuation line with different extraLabels
    // than for previous lines, the new extra labels won't be set. But this is not something that should be done anyway,
    // so it should not be considered a problem...
    //
    
    if (!reuseLabels && null != extraLabels) {
      labels.putAll(extraLabels);
    
      //
      // Remove labels with null values
      //
      // FIXME(hbs): may be removed when dummy tokens have disappeared
      //
      
      if (extraLabels.containsValue(null)) {
        Set<Entry<String,String>> entries = extraLabels.entrySet();
        
        while(labels.containsValue(null)) {
          for (Entry<String,String> entry: entries) {
            if (null == entry.getValue()) {
              labels.remove(entry.getKey());
            }
          }
        }        
      }
    }
    
    //
    // Extract value
    //
    
    String valuestr = str.substring(idx);

    if (valuestr.length() > maxValueSize) {
      throw new ParseException("Value too large at for GTS " + (null != encoder ? GTSHelper.buildSelector(encoder.getMetadata()) : ""), 0);
    }
    
    Object value = parseValue(valuestr);
      
    if (null == value) {
      throw new ParseException("Unable to parse value '" + valuestr + "'", 0);
    }
    
    // Allocate a new Encoder if need be, with a base timestamp of 0L.
    if (null == encoder || !name.equals(encoder.getName()) || !labels.equals(encoder.getMetadata().getLabels())) {
      encoder = new GTSEncoder(0L);
      encoder.setName(name);
      //encoder.setLabels(labels);
      encoder.getMetadata().setLabels(labels);
      if (null != attributes) {
        encoder.getMetadata().setAttributes(attributes);
      }
    }
    
    encoder.addValue(timestamp, location, elevation, value);
    
    return encoder;
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str) throws ParseException, IOException {
    return parse(encoder, str, null);
  }
  
  public static Object parseValue(String valuestr) throws ParseException {
    
    Object value;
    
    try {
      if (('\'' == valuestr.charAt(0) && valuestr.endsWith("'"))
          || ('"' == valuestr.charAt(0) && valuestr.endsWith("\""))) {
        value = valuestr.substring(1, valuestr.length() - 1);
        if (((String)value).contains("%")) {
          try {
            value = URLDecoder.decode((String) value, "UTF-8");
          } catch (UnsupportedEncodingException uee) {
            // Can't happen, we're using UTF-8
          }
        }        
      } else if ("t".equalsIgnoreCase(valuestr)
                 || "true".equalsIgnoreCase(valuestr)) {
        value = Boolean.TRUE;
      } else if ("f".equalsIgnoreCase(valuestr)
                 || "false".equalsIgnoreCase(valuestr)) {
        value = Boolean.FALSE;
      //
      // FIXME(hbs): add support for quaternions, for hex values???
      //
      } else {
        boolean likelydouble = UnsafeString.isDouble(valuestr);
        
        if (!likelydouble) {
          //value = Long.valueOf(valuestr);
          value = Long.parseLong(valuestr);          
        } else {
          //
          // If the double does not contain an 'E' or an 'N' or an 'n' (scientific notation or NaN or Infinity)
          // we use the following heuristic to determine if we should return a Double or a BigDecimal,
          // since we encode a mantissa of up to 46 bits, we will return a BigDecimal only if the size of the
          // input is <= 15 digits (log(46)/log(10) = 13.84 plus one digit for the dot plus one to tolerate values starting with low digits).
          // This will have the following caveats:
          //    * leading or trailing 0s should be discarded, this heuristic won't do that
          //    * negative values with 14 or more digits will be returned as Double, but we don't want to parse the
          //    first char.
          //    
          if (valuestr.length() <= 15 && UnsafeString.mayBeDecimalDouble(valuestr)) {
            value = new BigDecimal(valuestr);
          } else {
            value = Double.parseDouble(valuestr);
          }
        }
      }      
    } catch (Exception e) {
      throw new ParseException(valuestr, 0);
    }
    
    return value;
  }
  
  private static final Pattern STRING_VALUE_RE = Pattern.compile("^['\"].*['\"]$");
  private static final Pattern BOOLEAN_VALUE_RE = Pattern.compile("^(T|F|true|false)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern LONG_VALUE_RE = Pattern.compile("^[+-]?[0-9]+$");
  private static final Pattern DOUBLE_VALUE_RE = Pattern.compile("^[+-]?([0-9]+)\\.([0-9]+)$");  

  public static Object parseValue_regexp(String valuestr) throws ParseException {
    
    Object value;
    
    Matcher valuematcher = DOUBLE_VALUE_RE.matcher(valuestr);

    if (valuematcher.matches()) {
      // FIXME(hbs): maybe find a better heuristic to determine if we should
      // create a BigDecimal or a Double. BigDecimal is only meaningful if its encoding
      // will be less than 8 bytes.
      
      //value = Double.valueOf(valuestr);
      if (valuematcher.group(1).length() < 10 && valuematcher.group(2).length() < 10) {
        value = new BigDecimal(valuestr);
      } else {
        //value = Double.valueOf(valuestr);
        value = Double.parseDouble(valuestr);
      }
    } else {
      valuematcher = LONG_VALUE_RE.matcher(valuestr);
      
      if (valuematcher.matches()) {
        //value = Long.valueOf(valuestr);
        value = Long.parseLong(valuestr);
      } else {
        valuematcher = STRING_VALUE_RE.matcher(valuestr);
        
        if (valuematcher.matches()) {
          value = valuestr.substring(1, valuestr.length() - 1);
        } else {
          valuematcher = BOOLEAN_VALUE_RE.matcher(valuestr);
          
          if (valuematcher.matches()) {
            if ('t' == valuestr.charAt(0) || 'T' == valuestr.charAt(0)) {
              value = Boolean.TRUE;
            } else {
              value = Boolean.FALSE;
            }
          } else {
            throw new ParseException(valuestr, 0);
          }
        }
      }
    }

    return value;
  }
  /**
   * Parses the string representation of labels.
   * The representation is composed of name=value pairs separated by commas (',').
   * Values are expected to be UTF-8 strings percent-encoded.
   * 
   * @param str
   * @return
   * @throws InvalidFormatException if a label name is incorrect.
   */
  public static Map<String,String> parseLabels(int initialCapacity, String str) throws ParseException {
    
    //
    // Parse as selectors. We'll throw an exception if we encounter a regexp selector
    //
    
    Map<String,String> selectors = parseLabelsSelectors(str);
    Map<String,String> labels = new HashMap<String,String>(selectors.size() + initialCapacity);

    for (Entry<String, String> entry: selectors.entrySet()) {
      //
      // Check that selector is an exact match which would means
      // the syntax was label=value
      //
      
      if ('=' != entry.getValue().charAt(0)) {
        throw new ParseException(entry.getValue(), 0);
      }
      
      labels.put(entry.getKey(), entry.getValue().substring(1));
    }
    
    return labels;
  }
  
  public static Map<String,String> parseLabels(String str) throws ParseException {
    return parseLabels(0, str);
  }
      
  /**
   * Check that the metric name is conformant to its syntax.
   * 
   * @param name
   * @return
   */
  
  /**
   * Compute a class Id (metric name Id) using SipHash and the given key.
   * 
   * We compute the class Id in two phases.
   * 
   * During the first phase we compute the SipHash of the class name and
   * the SipHash of the reversed class name.
   * 
   * Then we compute the SipHash of the concatenation of those two hashes.
   * 
   * This is done to mitigate potential collisions in SipHash. We assume that
   * if a collision occurs for two class names CLASS0 and CLASS1, no collision will
   * occur for the reversed class names (i.e. 0SSALC and 1SSALC), and that no collision
   * will occur on concatenations of hash and hash of reversed input.
   * 
   * We waste some CPU cycles compared to just computing a SipHash of the class name
   * but this will ensure we do not risk synthezied collisions in classIds.
   * 
   * In the highly unlikely event of a collision, we'll offer 12 months of free service to the
   * customer who finds it!.
   * 
   * @param key 128 bits SipHash key to use
   * @param name Name of metric to hash
   */
  public static final long classId(byte[] key, String name) {
    long[] sipkey = SipHashInline.getKey(key);
    return classId(sipkey, name);
  }
  
  public static final long classId(long[] key, String name) {
    return classId(key[0], key[1], name);
  }
  
  public static final long classId(long k0, long k1, String name) {
    CharsetEncoder ce = Charsets.UTF_8.newEncoder();

    ce.onMalformedInput(CodingErrorAction.REPLACE)
    .onUnmappableCharacter(CodingErrorAction.REPLACE)
    .reset();

    byte[] ba = new byte[(int) ((double) ce.maxBytesPerChar() * name.length())];

    int blen = ((ArrayEncoder)ce).encode(UnsafeString.getChars(name), UnsafeString.getOffset(name), name.length(), ba);

    return SipHashInline.hash24_palindromic(k0, k1, ba, 0, blen);
  }
  
  /**
   * Compute a gts Id from given classId/labelsId.
   * 
   * We compute the palindromic SipHash of 'GTS:' + <classId> + ':' + <labelsId>
   * 
   * @param classId
   * @param labelsId
   * @return
   */
  public static final long gtsId(long[] key, long classId, long labelsId) {
    byte[] buf = new byte[8 + 8 + 2 + 3];
    buf[0] = 'G';
    buf[1] = 'T';
    buf[2] = 'S';
    buf[3] = ':';
    for (int i = 0; i < 8; i++) {
      buf[11-i] = (byte) (classId & 0xffL);
      classId >>>= 8;
    }
    
    buf[12] = ':';
    
    for (int i = 0; i < 8; i++) {
      buf[20-i] = (byte) (labelsId & 0xffL);
      labelsId >>>= 8;
    }
    
    return SipHashInline.hash24_palindromic(key[0], key[1], buf);
  }
  
  /**
   * Compute the class Id of a Geo Time Serie instance.
   * 
   * @param key 128 bits SipHash key to use
   * @param gts GeoTimeSerie instance for which to compute the classId
   * @return
   */
  public static final long classId(byte[] key, GeoTimeSerie gts) {
    return classId(key, gts.getName());
  }
  
  /**
   * Compute a labels Id (unique hash of labels) using SipHash and the given key.
   * 
   * The label Id is computed in two phases.
   * 
   * During the first phase, we compute the SipHash of both label names and values.
   * We then sort those pairs of hashes in ascending order (name hashes, then associated values).
   * Note that unless there is a collision in name hashes, we should only have different name hashes
   * since a label name can only appear once in the labels map.
   * 
   * Once the hashes are sorted, we compute the SipHash of the ids array.
   * 
   * In case of label names collisions (label0 and label1), the labels Id will be identical for the value set of label0 and label1,
   * i.e. the labels Id will be the same for label0=X,label1=Y and label0=Y,label1=X.
   * 
   * In case of label values collisions(X and Y), the labels Id will be identical when the colliding values are affected to the same label,
   * i.e. the labels Id of label0=X will be the same as that of label0=Y.
   * 
   * Even though those cases have a non zero probability of happening, we deem them highly improbable as there is a strong probability
   * that the collision happens with some random data which would probably not be used as a label name or value which tend
   * to have meaning.
   * 
   * And if our hypothesis on randomness is not verified, we'll have the pleasure of showing the world a legible SipHash
   * collision :-) And we'll offer 6 month of subscription to the customer who finds it!
   * 
   * @param key 128 bit SipHash key to use
   * @param labels Map of label names to label values
   */
  
  public static final long labelsId(byte[] key, Map<String,String> labels) {
    long[] sipkey = SipHashInline.getKey(key);
    return labelsId(sipkey, labels);
  }
  
  public static final long labelsId(long[] sipkey, Map<String,String> labels) {
    return labelsId(sipkey[0], sipkey[1], labels);
  }
  
  public static final long labelsId(long sipkey0, long sipkey1, Map<String,String> labels) {
    
    //
    // Allocate a CharsetEncoder.
    // Implementation is a sun.nio.cs.UTF_8$Encoder which implements ArrayEncoder
    //
    
    CharsetEncoder ce = Charsets.UTF_8.newEncoder();
    
    //
    // Allocate arrays large enough for most cases
    //
    
    int calen = 64;
    byte[] ba = new byte[(int) ((double) ce.maxBytesPerChar() * calen)];
    //char[] ca = new char[64];
    
    
    //
    // Allocate an array to hold both name and value hashes
    //
    
    long[] hashes = new long[labels.size() * 2];
    
    //
    // Compute hash for each name and each value
    //
    
    int idx = 0;
    
    for (Entry<String, String> entry: labels.entrySet()) {
      String ekey = entry.getKey();
      String eval = entry.getValue();
      
      int klen = ekey.length();
      int vlen = eval.length();
      
      if (klen > calen || vlen > calen) {
        //ca = new char[Math.max(klen,vlen)];
        calen = Math.max(klen, vlen);
        ba = new byte[(int) ((double) ce.maxBytesPerChar() * calen)];
      }
      
      //ekey.getChars(0, klen, ca, 0);
      ce.onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)
      .reset();

      //int blen = ((ArrayEncoder)ce).encode(ca, 0, klen, ba);
      int blen = ((ArrayEncoder)ce).encode(UnsafeString.getChars(ekey), UnsafeString.getOffset(ekey), klen, ba);

      hashes[idx] = SipHashInline.hash24_palindromic(sipkey0, sipkey1, ba, 0, blen);
      
      //eval.getChars(0, vlen, ca, 0);
      ce.onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)
      .reset();

      //blen = ((ArrayEncoder)ce).encode(ca, 0, vlen, ba);
      blen = ((ArrayEncoder)ce).encode(UnsafeString.getChars(eval), UnsafeString.getOffset(eval), vlen, ba);
      
      hashes[idx+1] = SipHashInline.hash24_palindromic(sipkey0, sipkey1, ba, 0, blen);
      idx+=2;
    }
    
    //
    // Sort array by label hash then value hash.
    // Given the normally reduced number of labels, we can use a simple bubble sort...
    //
    
    for (int i = 0; i < labels.size() - 1; i++) {
      for (int j = i + 1; j < labels.size(); j++) {
        if (hashes[i * 2] > hashes[j * 2]) {
          //
          // We need to swap name and value ids at i and j
          //
          long tmp = hashes[j * 2];
          hashes[j * 2] = hashes[i * 2];
          hashes[i * 2] = tmp;
          tmp = hashes[j * 2 + 1];
          hashes[j * 2 + 1] = hashes[i * 2 + 1];
          hashes[i * 2 + 1] = tmp;                    
        } else if (hashes[i * 2] == hashes[j * 2] && hashes[i * 2 + 1] > hashes[j * 2 + 1]) {
          //
          // We need to swap value ids at i and j
          //
          long tmp = hashes[j * 2 + 1];
          hashes[j * 2 + 1] = hashes[i * 2 + 1];
          hashes[i * 2 + 1] = tmp;
        }
      }
    }
    
    //
    // Now compute the SipHash of all the longs in the order we just determined
    // 
    
    byte[] buf = new byte[hashes.length * 8];
    //ByteBuffer bb = ByteBuffer.wrap(buf);
    //bb.order(ByteOrder.BIG_ENDIAN);

    idx = 0;
    
    for (long hash: hashes) {
      buf[idx++] = (byte) ((hash >> 56) & 0xff);
      buf[idx++] = (byte) ((hash >> 48) & 0xff);
      buf[idx++] = (byte) ((hash >> 40) & 0xff);
      buf[idx++] = (byte) ((hash >> 32) & 0xff);
      buf[idx++] = (byte) ((hash >> 24) & 0xff);
      buf[idx++] = (byte) ((hash >> 16) & 0xff);
      buf[idx++] = (byte) ((hash >> 8) & 0xff);
      buf[idx++] = (byte) (hash & 0xff);
      //bb.putLong(hash);
    }
    
    //return SipHashInline.hash24(sipkey[0], sipkey[1], buf, 0, buf.length);
    return SipHashInline.hash24_palindromic(sipkey0, sipkey1, buf, 0, buf.length);
  }

  public static final long labelsId_slow(byte[] key, Map<String,String> labels) {
    //
    // Allocate an array to hold both name and value hashes
    //
    
    long[] hashes = new long[labels.size() * 2];
    
    //
    // Compute hash for each name and each value
    //
    
    int idx = 0;
    
    long[] sipkey = SipHashInline.getKey(key);
    
    for (Entry<String, String> entry: labels.entrySet()) {
      hashes[idx] = SipHashInline.hash24_palindromic(sipkey[0], sipkey[1], entry.getKey().getBytes(Charsets.UTF_8));
      hashes[idx+1] = SipHashInline.hash24_palindromic(sipkey[0], sipkey[1], entry.getValue().getBytes(Charsets.UTF_8));
      idx+=2;
    }
    
    //
    // Sort array by label hash then value hash.
    // Given the normally reduced number of labels, we can use a simple bubble sort...
    //
    
    for (int i = 0; i < labels.size() - 1; i++) {
      for (int j = i + 1; j < labels.size(); j++) {
        if (hashes[i * 2] > hashes[j * 2]) {
          //
          // We need to swap name and value ids at i and j
          //
          long tmp = hashes[j * 2];
          hashes[j * 2] = hashes[i * 2];
          hashes[i * 2] = tmp;
          tmp = hashes[j * 2 + 1];
          hashes[j * 2 + 1] = hashes[i * 2 + 1];
          hashes[i * 2 + 1] = tmp;                    
        } else if (hashes[i * 2] == hashes[j * 2] && hashes[i * 2 + 1] > hashes[j * 2 + 1]) {
          //
          // We need to swap value ids at i and j
          //
          long tmp = hashes[j * 2 + 1];
          hashes[j * 2 + 1] = hashes[i * 2 + 1];
          hashes[i * 2 + 1] = tmp;
        }
      }
    }
    
    //
    // Now compute the SipHash of all the longs in the order we just determined
    // 
    
    byte[] buf = new byte[hashes.length * 8];
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.order(ByteOrder.BIG_ENDIAN);

    for (long hash: hashes) {
      bb.putLong(hash);
    }
    
    //return SipHashInline.hash24(sipkey[0], sipkey[1], buf, 0, buf.length);
    return SipHashInline.hash24_palindromic(sipkey[0], sipkey[1], buf, 0, buf.length);
  }
  
  /**
   * Compute the labels Id of a Geo Time Serie instance.
   * 
   * @param key 128 bits SipHash key to use
   * @param gts GeoTimeSerie instance for which to compute the labels Id
   * @return
   */
  public static final long labelsId(byte[] key, GeoTimeSerie gts) {
    return labelsId(key, gts.getLabels());
  }

  /**
   * Convert a GTS Id packed as a BigInteger into an array of bytes
   * containing classId/labelsId in big endian representation
   * @param bi
   * @return
   */
  public static byte[] unpackGTSId(BigInteger bi) {
    byte[] bytes = bi.toByteArray();
    
    if (bytes.length < 16) {
      byte[] tmp = new byte[16];
      if (bi.signum() < 0) {
        Arrays.fill(tmp, (byte) 0xff);
      }
      System.arraycopy(bytes, 0, tmp, tmp.length - bytes.length, bytes.length);
      return tmp;
    } else {
      return bytes;
    }
  }

  public static byte[] unpackGTSId(String s) {
    byte[] bytes = new byte[16];
    
    if (8 != s.length()) {
      return bytes;
    }
    
    char[] c = UnsafeString.getChars(s);

    for (int i = 0; i < 8; i++) {
      bytes[i * 2] = (byte) ((c[i] >> 8) & 0xFF);
      bytes[1 + i * 2] = (byte) (c[i] & 0xFF);
    }
    
    return bytes;
  }
  
  public static long[] unpackGTSIdLongs(String s) {
    long[] clslbls = new long[2];

    char[] c = UnsafeString.getChars(s);
    for (int i = 0; i < 4; i++) {
      clslbls[0] <<= 16;
      clslbls[0] |= (c[i] & 0xFFFFL) & 0xFFFFL;
      clslbls[1] <<= 16;
      clslbls[1] |= (c[i + 4] & 0xFFFFL) & 0xFFFFL;
    }


    return clslbls;
  }
  
  public static String gtsIdToString(long classId, long labelsId) {
    return gtsIdToString(classId, labelsId, true);
  }
  
  public static String gtsIdToString(long classId, long labelsId, boolean intern) {
    String s = new String("01234567");
    
    // This way we don't create a char array twice...
    char[] c = UnsafeString.getChars(s);
    
    long x = classId;
    long y = labelsId;
    
    for (int i = 3; i >= 0; i--) {
      c[i] = (char) (x & 0xffffL);
      c[i+4] = (char) (y & 0xffffL); 
      x >>>= 16;
      y >>>= 16;
    }
    
    if (intern) {
      return s.intern();
    } else {
      return s;
    }
  }
  
  public static long[] stringToGTSId(String s) {    
    char[] c = s.toCharArray();
    
    long x = 0L;
    long y = 0L;
    
    for (int i = 0; i < 4; i++) {
      x <<= 16;
      x |= (c[i] & 0xffffL);
      y <<= 16;
      y |= (c[i + 4] & 0xffffL);
    }
    
    long[] clslbls = new long[2];
    clslbls[0] = x;
    clslbls[1] = y;

    return clslbls;
  }
  
  public static void fillGTSIds(byte[] bytes, int offset, long classId, long labelsId) {   
    long id = classId;
    int idx = offset;
    
    bytes[idx++] = (byte) ((id >> 56) & 0xff);
    bytes[idx++] = (byte) ((id >> 48) & 0xff);
    bytes[idx++] = (byte) ((id >> 40) & 0xff);
    bytes[idx++] = (byte) ((id >> 32) & 0xff);
    bytes[idx++] = (byte) ((id >> 24) & 0xff);
    bytes[idx++] = (byte) ((id >> 16) & 0xff);
    bytes[idx++] = (byte) ((id >> 8) & 0xff);
    bytes[idx++] = (byte) (id & 0xff);
    
    id = labelsId;

    bytes[idx++] = (byte) ((id >> 56) & 0xff);
    bytes[idx++] = (byte) ((id >> 48) & 0xff);
    bytes[idx++] = (byte) ((id >> 40) & 0xff);
    bytes[idx++] = (byte) ((id >> 32) & 0xff);
    bytes[idx++] = (byte) ((id >> 24) & 0xff);
    bytes[idx++] = (byte) ((id >> 16) & 0xff);
    bytes[idx++] = (byte) ((id >> 8) & 0xff);
    bytes[idx++] = (byte) (id & 0xff);
  }
  
  /**
   * Parse labels selectors and return a map of label name to selector.
   * 
   * The syntaxt of the 'selectors' String is:
   * 
   * NAME<TYPE>VALUE,NAME<TYPE>VALUE,...
   * 
   * where <TYPE> is either '=' for exact matches or '~' for regular expression matches.
   * NAME and VALUE are percent-encoded UTF-8 Strings.
   * 
   * @param selectors 
   * @return A map from label name to selector.
   */
  public static final Map<String,String> parseLabelsSelectors(String selectors) throws ParseException {
    //
    // Split selectors on ',' boundaries
    //
    
    //String[] tokens = selectors.split(",");
    String[] tokens = UnsafeString.split(selectors, ',');
    
    //Iterable<String> tokens = Splitter.on(",").split(selectors);
    
    //
    // Loop over the tokens
    //
    
    Map<String,String> result = new HashMap<String,String>(tokens.length);
    
    for (String token: tokens) {
      
      token = token.trim();
      
      if ("".equals(token)) {
        continue;
      }
      
      //
      // Split on '=' or '~'
      //
      
      boolean exact = true;
      
      Iterable<String> stokens;
      
      String[] subtokens;
      
      if (token.contains("=")) {
        exact = true;
        //subtokens = token.split("=");
        subtokens = UnsafeString.split(token, '=');
        
        //stokens = Splitter.on("=").split(token);
      } else if (token.contains("~")){
        exact = false;
        //subtokens = token.split("~");
        subtokens = UnsafeString.split(token, '~');
        
        //stokens = Splitter.on("~").split(token);
      } else {
        throw new ParseException(token,0);
      }
      
      //Iterator<String> iter = stokens.iterator();
      
      //String name = iter.next();
      //String value = iter.hasNext() ? iter.next() : "";
      String name = subtokens[0];
      String value = subtokens.length > 1 ? subtokens[1] : "";
      
      try {
        if (name.contains("%")) {
          name = URLDecoder.decode(name, "UTF-8");
        }
        
        if (value.contains("%")) {
          value = URLDecoder.decode(value, "UTF-8");
        }        
      } catch (UnsupportedEncodingException uee) {
        // Can't happen, we're using UTF-8 which is a standard JVM encoding.
      }
            
      result.put(name, (exact ? "=" : "~") + value);
    }
    
    return result;
  }
  
  /**
   * Return patterns for matching class and labels
   * The class selector is associated with the 'null' key.
   * The labels selectors are associated with the label name they're associated with
   * 
   * @param classLabelsSelectionString A string representation of a class/labels selector
   * @return
   * @throws ParseException
   */
  public static Map<String,Pattern> patternsFromSelectors(String classLabelsSelectionString) throws ParseException {
    String classSelector = classLabelsSelectionString.replaceAll("\\{.*$", "");
    String labelsSelectorsString = classLabelsSelectionString.replaceAll("^.*\\{","").replaceAll("\\}.*$", "");
    
    Map<String,String> labelsSelectors = parseLabelsSelectors(labelsSelectorsString);
    
    Map<String,Pattern> patterns = new HashMap<String,Pattern>();
    
    if (classSelector.contains("%")) {
      try {      
        classSelector = URLDecoder.decode(classSelector, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        // Can't happen, we're using UTF-8
      }      
    }

    if ('=' == classSelector.charAt(0)) {
      patterns.put(null, Pattern.compile(Pattern.quote(classSelector.substring(1))));            
    } else if ('~' == classSelector.charAt(0)) {
      patterns.put(null, Pattern.compile(classSelector.substring(1)));      
    } else {
      patterns.put(null, Pattern.compile(Pattern.quote(classSelector)));      
    }
    
    for (Entry<String,String> entry: labelsSelectors.entrySet()) {
      if ('=' == entry.getValue().charAt(0)) {
        patterns.put(entry.getKey(), Pattern.compile(Pattern.quote(entry.getValue().substring(1))));
      } else {
        patterns.put(entry.getKey(), Pattern.compile(entry.getValue().substring(1)));
      }
    }
    
    return patterns;
  }
  
  public static Iterator<String> valueRepresentationIterator(final GeoTimeSerie gts) {
    return new Iterator<String>() {
      int idx = 0;
      
      @Override
      public boolean hasNext() {
        return idx < gts.values;
      }
      @Override
      public String next() {
        Object value;
        if (TYPE.LONG == gts.type) {
          value = gts.longValues[idx];
        } else if (TYPE.DOUBLE == gts.type) {
          value = gts.doubleValues[idx];
        } else if (TYPE.STRING == gts.type) {
          value = gts.stringValues[idx];
        } else if (TYPE.BOOLEAN == gts.type) {
          value = gts.booleanValues.get(idx);
        } else {
          value = null;
        }

        String repr = tickToString(gts.ticks[idx], null != gts.locations ? gts.locations[idx] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[idx] : GeoTimeSerie.NO_ELEVATION, value);
        idx++;
        
        return repr;
      }
      @Override
      public void remove() {}
    };
  }
  
  /**
   * Return a string representation of a GTS measurement at 'tick'
   * 
   * @return
   */
  public static String tickToString(StringBuilder clslbls, long timestamp, long location, long elevation, Object value) {
    try {
      StringBuilder sb = new StringBuilder();
      
      sb.append(timestamp);
      sb.append("/");
      if (GeoTimeSerie.NO_LOCATION != location) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(location);
        sb.append(latlon[0]);
        sb.append(":");
        sb.append(latlon[1]);
      }
      sb.append("/");
      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        sb.append(elevation);
      }
      sb.append(" ");
      
      if (null != clslbls && clslbls.length() > 0) {
        sb.append(clslbls);
        sb.append(" ");
      }
      
      encodeValue(sb, value);
      return sb.toString();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String tickToString(long timestamp, long location, long elevation, Object value) {
    return tickToString(null, timestamp, location, elevation, value);
  }

  public static void encodeValue(StringBuilder sb, Object value) {
    if (value instanceof Long || value instanceof Double) {
      sb.append(value);
    } else if (value instanceof BigDecimal) {
      sb.append(((BigDecimal) value).toPlainString());
      if (((BigDecimal) value).scale() <= 0) {
        sb.append(".0");
      }
    } else if (value instanceof Boolean) {
      sb.append(((Boolean) value).equals(Boolean.TRUE) ? "T" : "F");
    } else if (value instanceof String) {
      sb.append("'");
      try {
        String encoded = WarpURLEncoder.encode((String) value, "UTF-8");
        sb.append(encoded);
      } catch (UnsupportedEncodingException uee) {
        // Won't happen
      }
      sb.append("'");
    }
  }
  
  public static void encodeName(StringBuilder sb, String name) {
    if (null == name) {
      return;
    }
    try {
      String encoded = WarpURLEncoder.encode(name, "UTF-8");
      sb.append(encoded);
    } catch (UnsupportedEncodingException uee) {      
    }
  }
  
  /**
   * Merge 'gts' into 'base' the slow way, i.e. copying values one at a time.
   * 
   * If 'gts' and 'base' differ in type, no merging takes place, unless 'base' is empty.
   * Values/locations/elevations in gts override those in base if they occur at the same tick.
   * 
   * No check of name or labels is done, the name and labels of 'base' are not modified.
   * 
   * @param base GTS instance into which other values should be merged
   * @param gts GTS instance whose values/locations/elevations should be merged.
   * @param overwrite Flag indicating whether or not to overwrite existing value at identical timestamp
   * 
   * @return base
   */
  public static GeoTimeSerie slowmerge(GeoTimeSerie base, GeoTimeSerie gts, boolean overwrite) {
    GeoTimeSerie.TYPE baseType = base.getType();
    GeoTimeSerie.TYPE gtsType = gts.getType();
    
    if (TYPE.UNDEFINED.equals(baseType) || baseType.equals(gtsType)) {
      for (int i = 0; i < gts.values; i++) {
        GTSHelper.setValue(base, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), overwrite);        
      }
    }

    return base;
  }

  /**
   * Merge 'gts' into 'base'.
   * 
   * If 'gts' and 'base' differ in type, no merging takes place, unless 'base' is empty.
   * No deduplication is done if the same tick is present in 'base' and 'gts'.
   * 
   * No check of name or labels is done, the name and labels of 'base' are not modified.
   * 
   * @param base GTS instance into which other values should be merged
   * @param gts GTS instance whose values/locations/elevations should be merged.
   * 
   * @return base
   */

  public static GeoTimeSerie merge(GeoTimeSerie base, GeoTimeSerie gts) {
    GeoTimeSerie.TYPE baseType = base.getType();
    GeoTimeSerie.TYPE gtsType = gts.getType();
    
    if (TYPE.UNDEFINED.equals(baseType) || baseType.equals(gtsType)) {
      
      if (0 == gts.values) {
        return base;
      }
      
      if (null == base.ticks) {
        base.ticks = Arrays.copyOf(gts.ticks, gts.values);
      } else {
        base.ticks = Arrays.copyOf(base.ticks, base.values + gts.values);
        System.arraycopy(gts.ticks, 0, base.ticks, base.values, gts.values);
      }

      if (null == base.locations) {
        if (null != gts.locations) {
          base.locations = new long[base.ticks.length];
          Arrays.fill(base.locations, GeoTimeSerie.NO_LOCATION);
          System.arraycopy(gts.locations, 0, base.locations, base.values, gts.values);
        }
      } else {
        base.locations = Arrays.copyOf(base.locations, base.values + gts.values);
        if (null != gts.locations) {
          System.arraycopy(gts.locations, 0, base.locations, base.values, gts.values);
        } else {
          Arrays.fill(base.locations, base.values, base.values + gts.values, GeoTimeSerie.NO_LOCATION);
        }
      }

      if (null == base.elevations) {
        if (null != gts.elevations) {
          base.elevations = new long[base.ticks.length];
          Arrays.fill(base.elevations, GeoTimeSerie.NO_ELEVATION);
          System.arraycopy(gts.elevations, 0, base.elevations, base.values, gts.values);
        }
      } else {
        base.elevations = Arrays.copyOf(base.elevations, base.values + gts.values);
        if (null != gts.elevations) {
          System.arraycopy(gts.elevations, 0, base.elevations, base.values, gts.values);
        } else {
          Arrays.fill(base.elevations, base.values, base.values + gts.values, GeoTimeSerie.NO_ELEVATION);
        }
      }

      switch (gtsType) {
        case LONG:
          base.type = TYPE.LONG;
          if (null == base.longValues) {
            base.longValues = Arrays.copyOf(gts.longValues, gts.values);
          } else {
            base.longValues = Arrays.copyOf(base.longValues, base.values + gts.values);
            System.arraycopy(gts.longValues, 0, base.longValues, base.values, gts.values);
          }
          break;
        case DOUBLE:
          base.type = TYPE.DOUBLE;
          if (null == base.doubleValues) {
            base.doubleValues = Arrays.copyOf(gts.doubleValues, gts.values);
          } else {
            base.doubleValues = Arrays.copyOf(base.doubleValues, base.values + gts.values);
            System.arraycopy(gts.doubleValues, 0, base.doubleValues, base.values, gts.values);
          }
          break;
        case STRING:
          base.type = TYPE.STRING;
          if (null == base.stringValues) {
            base.stringValues = Arrays.copyOf(gts.stringValues, gts.values);
          } else {
            base.stringValues = Arrays.copyOf(base.stringValues, base.values + gts.values);
            System.arraycopy(gts.stringValues, 0, base.stringValues, base.values, gts.values);
          }
          break;
        case BOOLEAN:
          base.type = TYPE.BOOLEAN;
          if (null == base.booleanValues) {
            base.booleanValues = (BitSet) gts.booleanValues.clone();
          } else {
            for (int i = 0; i < gts.values; i++) {
              base.booleanValues.set(base.values + i, gts.booleanValues.get(i));
            }
          }
          break;
      }
      
      base.values = base.values + gts.values;
      
    }

    base.sorted = false;
    
    return base;
  }

  /**
   * Merge GeoTimeSerie instances using GTSEncoders.
   * This is noticeably faster than what 'merge' is doing.
   * 
   * @param series List of series to merge, the first one will be considered the base. Its metadata will be used.
   * @return The merged series.
   */
  public static GeoTimeSerie mergeViaEncoders(List<GeoTimeSerie> series) throws IOException {
    //
    // We merge the GTS but do not use GTSHelper#merge
    // as it is less efficient than the use of GTSDecoder
    //
    
    GTSEncoder encoder = new GTSEncoder(0L);

    try {
      for (int i = 0; i < series.size(); i++) {
        GeoTimeSerie gts = series.get(i);

        if (0 == i) {
          encoder.setMetadata(gts.getMetadata());
        }
        encoder.encode(gts);
      }
    } catch (IOException ioe) {
      throw new IOException(ioe);
    }
    
    //return encoder.getUnsafeDecoder(false).decode();
    return encoder.getDecoder(true).decode();
  }
  
  /**
   * Fill missing values/locations/elevations in a bucketized GTS with the previously
   * encountered one.
   * 
   * Returns a filled clone of 'gts'. 'gts' is not modified.
   * 
   * @param gts GeoTimeSerie to fill
   * @return
   */
  public static GeoTimeSerie fillprevious(GeoTimeSerie gts) {
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized, do nothing
    //
        
    if (!isBucketized(filled)) {
      return filled;
    }
    
    //
    // Sort GTS in natural ordering of ticks
    //
    
    sort(filled, false);
    
    //
    // Loop on ticks
    //
    
    int nticks = filled.values;
    
    //
    // Modify size hint so we only allocate once
    //

    if (0 != nticks) {
      long firsttick = filled.ticks[0];
      filled.setSizeHint(1 + (int) ((filled.lastbucket - firsttick) / filled.bucketspan));
    }
    
    int idx = 0;
    int bucketidx = filled.bucketcount - 1;
    long bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
    
    Object prevValue = null;
    long prevLocation = GeoTimeSerie.NO_LOCATION;
    long prevElevation = GeoTimeSerie.NO_ELEVATION;
    
    long bucketoffset = filled.lastbucket % filled.bucketspan;
    
    while (bucketidx >= 0) {
      //
      // Only consider ticks which are valid buckets. We need to do
      // this test because values could have been added at invalid bucket
      // timestamps after the GTS has been bucketized.
      //
      
      while(idx < nticks && bucketoffset != (filled.ticks[idx] % filled.bucketspan)) {
        idx++;
      }
      
      if (idx >= nticks) {
        break;
      }
      
      while(bucketidx >= 0 && filled.ticks[idx] > bucketts) {
        if (null != prevValue) {
          setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
        }
        bucketidx--;
        bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      }
      
      //
      // We need to decrease bucketidx as ticks[idx] == bucketts
      // otherwise we would duplicate the existing values.
      //
      
      bucketidx--;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      
      prevValue = valueAtIndex(filled, idx);
      prevLocation = null != filled.locations ? filled.locations[idx] : GeoTimeSerie.NO_LOCATION;
      prevElevation = null != filled.elevations ? filled.elevations[idx] : GeoTimeSerie.NO_ELEVATION;
      
      idx++;
    }
    
    //
    // Finish filling up past the last seen value
    //

    while(bucketidx >= 0) {
      if (null != prevValue) {
        setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
      }
      bucketidx--;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;      
    }

    return filled;
  }

  /**
   * Fill missing values/locations/elevations in a bucketized GTS with the next
   * encountered one.
   * 
   * @param gts GeoTimeSerie to fill
   * @return A filled clone of 'gts'.
   */
  public static GeoTimeSerie fillnext(GeoTimeSerie gts) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized, do nothing
    //
        
    if (!isBucketized(filled)) {
      return filled;
    }
    
    //
    // Sort GTS in reverse ordering of ticks
    //
    
    sort(filled, true);
    
    //
    // Loop on ticks
    //
    
    int nticks = filled.values;

    //
    // Change size hint so we only do one allocation
    //
    
    if (0 != nticks) {
      long lasttick = filled.ticks[0];
      filled.setSizeHint(1 + (int) ((lasttick - (filled.lastbucket - filled.bucketcount * filled.bucketspan)) / filled.bucketspan));
    }

    int idx = 0;
    int bucketidx = 0;
    long bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
    
    Object prevValue = null;
    long prevLocation = GeoTimeSerie.NO_LOCATION;
    long prevElevation = GeoTimeSerie.NO_ELEVATION;
    
    long bucketoffset = filled.lastbucket % filled.bucketspan;

    while (bucketidx < filled.bucketcount) {
      //
      // Only consider ticks which are valid buckets. We need to do
      // this test because values could have been added at invalid bucket
      // timestamps after the GTS has been bucketized.
      //
      
      while(idx < nticks && bucketoffset != (filled.ticks[idx] % filled.bucketspan)) {
        idx++;
      }
      
      if (idx >= nticks) {
        break;
      }
      
      while(bucketidx >= 0 && filled.ticks[idx] < bucketts) {
        if (null != prevValue) {
          setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
        }
        bucketidx++;
        bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      }
      
      //
      // We need to increase bucketidx as ticks[idx] == bucketts
      // otherwise we would duplicate the existing values.
      //
      
      bucketidx++;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      
      prevValue = valueAtIndex(filled, idx);
      prevLocation = null != filled.locations ? filled.locations[idx] : GeoTimeSerie.NO_LOCATION;
      prevElevation = null != filled.elevations ? filled.elevations[idx] : GeoTimeSerie.NO_ELEVATION;
      
      idx++;
    }
    
    //
    // Finish filling up past the last seen value
    //

    while(bucketidx < filled.bucketcount) {
      if (null != prevValue) {
        setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
      }
      bucketidx++;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;      
    }

    return filled;
  }

  /**
   * Fill missing values/locations/elevations in a bucketized GTS with the
   * given location/elevation/value.
   * 
   * @param gts GeoTimeSerie to fill
   * @param location Location to use for filling
   * @param elevation Elevation to use for filling
   * @param value Value to use for filling 'gts'
   * 
   * @return A filled clone of 'gts'.
   */
  public static GeoTimeSerie fillvalue(GeoTimeSerie gts, long location, long elevation, Object value) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized or value is null, do nothing
    //
        
    if (!isBucketized(filled) || null == value) {
      return filled;
    }

    //
    // Force size hint since we know the GTS will contain 'bucketcount' ticks
    //
    
    filled.setSizeHint(filled.bucketcount);
    
    //
    // Sort 'filled'
    //
    
    sort(filled);
    
    int bucket = filled.bucketcount - 1;
    int idx = 0;
    int nticks = filled.values;
    
    while (bucket >= 0) {
      long bucketts = filled.lastbucket - bucket * filled.bucketspan;
      
      while((idx < nticks && filled.ticks[idx] > bucketts) || (idx >= nticks && bucket >= 0)) {
        setValue(filled, bucketts, location, elevation, value, false);
        bucket--;
        bucketts = filled.lastbucket - bucket * filled.bucketspan;
      }
    
      idx++;
      bucket--;
    }
  
    return filled;
  }

  /**
   * Fill the given ticks in a GTS with the elements provided.
   * 
   * If the GTS is bucketized, do nothing.
   * 
   * @param gts
   * @param location
   * @param elevation
   * @param value
   * @param ticks
   * @return
   */
  public static GeoTimeSerie fillticks(GeoTimeSerie gts, long location, long elevation, Object value, long[] ticks) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If value is null or GTS bucketized, do nothing
    //
        
    if (null == value || GTSHelper.isBucketized(filled)) {
      return filled;
    }
    
    //
    // Extract ticks from the GTS and sort them, we don't want to sort the values or location info
    //
    
    long[] gticks = filled.values > 0 ? Arrays.copyOf(filled.ticks, filled.values) : new long[0];
    
    Arrays.sort(gticks);
    
    //
    // Sort ticks
    //
    
    Arrays.sort(ticks);
    
    int gtsidx = 0;
    int tickidx = 0;
    
    int nvalues = filled.values;
        
    while(gtsidx < nvalues) {
      long tick = gticks[gtsidx];
      
      while(tickidx < ticks.length && ticks[tickidx] < tick) {
        GTSHelper.setValue(filled, ticks[tickidx], location, elevation, value, false);
        tickidx++;
      }
      
      gtsidx++;
    }
        
    while(tickidx < ticks.length) {
      GTSHelper.setValue(filled, ticks[tickidx], location, elevation, value, false);
      tickidx++;
    }
  
    return filled;
  }

  /**
   * Compensate resets by computing an offset each time a value decreased between two ticks.
   * 
   * If we have the following TS (from most recent to most ancient) :
   * 
   * 10 5 1 9 4 3 7 2 1
   * 
   * we detect 2 resets (one between values 9 and 1, another one between 7 and 3).
   * 
   * The filled GTS will be
   * 
   * 31 21 17 16 11 10 7 2 1
   * 
   * @param gts GTS instance to compensate resets for
   * @param decreasing If true, indicates that resets will have higher values than counter value (i.e. counter decreases)
   * @return
   */
  public static GeoTimeSerie compensateResets(GeoTimeSerie gts, boolean resethigher) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not of type LONG or DOUBLE, do noting
    //
        
    TYPE type = gts.getType();
    
    if (TYPE.LONG != type && TYPE.DOUBLE != type) {
      return filled;
    }
    
    //
    // Sort filled so ticks are in chronological orders
    //
    
    sort(filled);
    
    long lastl = TYPE.LONG == type ? filled.longValues[0] : 0L;
    long offsetl = 0L;
    
    double lastd = TYPE.DOUBLE == type ? filled.doubleValues[0] : 0.0D;
    double offsetd = 0.0D;
    
    for (int i = 1; i < filled.values; i++) {
      if (TYPE.LONG == type) {
        long value = filled.longValues[i];
        if (!resethigher) {
          if (value < lastl) {
            offsetl += lastl;
          }
          lastl = value;
        } else {
          if (value > lastl) {
            offsetl += lastl;
          }
          lastl = value;
        }
        filled.longValues[i] = value + offsetl;
      } else {
        double value = filled.doubleValues[i];
        if (!resethigher) {
          if (value < lastd) {
            offsetd += lastd; 
          }          
          lastd = value;
        } else {
          if (value > lastd) {
            offsetd += lastd; 
          }
          lastd = value;
        }
        filled.doubleValues[i] = value + offsetd;
      }
    }
    
    return filled;
  }
  
  public static boolean isBucketized(GeoTimeSerie gts) {    
    return 0 != gts.bucketcount && 0L != gts.bucketspan && 0L != gts.lastbucket; 
  }
  
  /**
   * Split a GTS into multiple GTS by cutting in 'quiet zones', i.e. intervals
   * of 'quietperiod' or more during which there were no measurements.
   *
   * If 'gts' has no values or if 'label' is already part of the labels of 'gts', then
   * the resulting list of GTS will only contain a clone of 'gts'.
   * 
   * @param gts GTS instance to split
   * @param quietperiod Mininum number of microseconds without values to consider a split. The previous value must be at least 'quietperiod' us ago.
   * @param minvalues Only produce GTS with more than 'minvalues' values, this is to ignore lone values
   * @param label Name to use for a label containing the GTS sequence (oldes GTS is 1, next is 2, ....). 'label' MUST NOT exist among the labels of 'gts'.
   * 
   * @return The list of resulting GTS.
   */
  public static List<GeoTimeSerie> timesplit(GeoTimeSerie gts, long quietperiod, int minvalues, String labelname) {
    
    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    //
    // If the input GTS already contains the sequence label or has no value, return it as is.
    //
    
    if (0 == gts.values || gts.hasLabel(labelname)) {
      series.add(gts.clone());
      return series;
    }
        
    //
    // Sort 'gts'
    //
    
    sort(gts, false);
    
    
    long lasttick = gts.ticks[0];
    
    int idx = 0;
        
    int gtsid = 1;
    
    //
    // Loop over ticks
    //
    
    GeoTimeSerie serie = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, 4);
    serie.setName(gts.getName());
    Map<String,String> labels = new HashMap<String,String>();
    labels.putAll(gts.getLabels());
    labels.put(labelname, Integer.toString(gtsid));
    serie.setLabels(labels);
    if (gts.getMetadata().getAttributesSize() > 0) {
      serie.getMetadata().setAttributes(new HashMap<String,String>(gts.getMetadata().getAttributes()));
    }
    
    while (idx < gts.values) {
      //
      // If current tick is further than 'quietperiod' away
      // from the previous one, init a new GTS.
      // If the current GTS contains at least 'minvalues' values,
      // add it as a result GTS.
      //
      
      if (gts.ticks[idx] - lasttick >= quietperiod) {
        if (serie.values > 0 && serie.values >= minvalues) {
          series.add(serie);
        }
        serie = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, 4);
        serie.setName(gts.getName());
        labels = new HashMap<String,String>();
        labels.putAll(gts.getLabels());
        gtsid++;
        labels.put(labelname, Integer.toString(gtsid));
        serie.setLabels(labels);
        if (gts.getMetadata().getAttributesSize() > 0) {
          serie.getMetadata().setAttributes(new HashMap<String,String>(gts.getMetadata().getAttributes()));
        }
      }

      Object value = GTSHelper.valueAtIndex(gts, idx);      
      GTSHelper.setValue(serie, gts.ticks[idx], null != gts.locations ? gts.locations[idx] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[idx] : GeoTimeSerie.NO_ELEVATION, value, false);
      
      lasttick = gts.ticks[idx];
      idx++;
    }
    
    if (serie.values > 0 && serie.values >= minvalues) {
      series.add(serie);
    }
    
    return series;
  }
  
  /**
   * Crops a bucketized GTS so it spans the largest interval with actual values.
   * This method simply clones non bucketized GTS.
   * For bucketized GTS, only values at bucket boundaries are kept.
   * 
   * @param gts GTS instance to crop.
   * 
   * @return A cropped version of GTS or a clone thereof if GTS was not bucketized.
   */
  public static GeoTimeSerie crop(GeoTimeSerie gts) {
    if (!isBucketized(gts)) {
      return gts.clone();
    }
    
    //
    // Sort GTS
    //
    
    sort(gts, false);
    
    long firstbucket = gts.lastbucket - (gts.bucketcount - 1) * gts.bucketspan;
    
    GeoTimeSerie cropped = new GeoTimeSerie(4);
    cropped.setMetadata(new Metadata(gts.getMetadata()));
    
    for (int i = 0; i < gts.values; i++) {
      if (gts.ticks[i] >= firstbucket
          && gts.ticks[i] <= gts.lastbucket
          && (gts.ticks[i] % gts.bucketspan == gts.lastbucket % gts.bucketspan)) {
        setValue(cropped, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), false);
      }
    }
    
    //
    // Compute lastbucket
    //
    
    cropped.bucketspan = gts.bucketspan;
    cropped.lastbucket = cropped.ticks[cropped.values - 1];
    cropped.bucketcount = 1 + (int) ((cropped.lastbucket - cropped.ticks[0]) / cropped.bucketspan);
    
    return cropped;
  }
  
  /**
   * Shifts a GTS instance by the given time delta.
   * All ticks will be shifted by 'delta'.
   * 
   * For bucketized GTS instances, all buckets will be shifted by 'delta'.
   * 
   * @param gts GTS instance to shift
   * @param delta Number of microsecondes to shift ticks by.
   * @return A shifted version of 'gts'.
   */
  public static GeoTimeSerie timeshift(GeoTimeSerie gts, long delta) {
    //
    // Clone gts.
    //
    
    GeoTimeSerie shifted = gts.clone();
    
    //
    // Shift ticks
    //
    
    for (int i = 0; i < shifted.values; i++) {
      shifted.ticks[i] = shifted.ticks[i] + delta;
    }
    
    //
    // Shift lastbucket if GTS is bucketized
    //
    
    if (isBucketized(shifted)) {
      shifted.lastbucket = shifted.lastbucket + delta;
    }
    
    return shifted;
  }
  
  /**
   * Produces a GTS instance similar to the original one except
   * that its ticks will have been modified to reflect their index
   * int the sequence, starting at 0.
   * 
   * @param gts
   * @return
   */
  public static GeoTimeSerie tickindex(GeoTimeSerie gts) {
    GeoTimeSerie indexed = gts.clone();
    
    for (int i = 0; i < indexed.values; i++) {
      indexed.ticks[i] = i;
    }

    //
    // Result cannot be bucketized
    //
    
    indexed.bucketcount = 0;
    indexed.bucketspan = 0;
    indexed.lastbucket = 0;
    
    return indexed;
  }
  
  /**
   * Removes all values from a GTS instance by setting its value count to 0
   * and its type to UNDEFINED
   * 
   * @param gts GTS instance to clear.
   * 
   */
  public static void clear(GeoTimeSerie gts) {
    gts.values = 0;
    gts.type = TYPE.UNDEFINED;
    gts.booleanValues = null;
    gts.locations = null;
    gts.elevations = null;
    gts.ticks = null;
    gts.doubleValues = null;
    gts.longValues = null;
    gts.stringValues = null;
  }
  
  public static void reset(GeoTimeSerie gts) {
    gts.values = 0;
    gts.type = TYPE.UNDEFINED;
    
    unbucketize(gts);
  }
  
  /**
   * Apply a mapper on a GeoTimeSerie instance and produce a new
   * GTS instance with the result of the mapper application.
   * 
   * @param gts GTS instance to apply the mapper on
   * @param mapper Mapper Function to use
   * @param prewindow Number of ticks or time interval to consider BEFORE each tick for which the computation is done.
   *                  If number is 0, don't consider any ticks prior to the current one.
   *                  If number is > 0, consider that many ticks prior to the current one.
   *                  If number is < 0, consider that number negated of microseconds prior to the current tick.
   *                  A delta comparison from the previous value will need a prewindow of 1 (1 tick before the current one).   *                  
   * @param postwindow Same meaning as 'prewindow' but for the interval AFTER each tick.
   * @param occurrences Number of times to apply map, 0 means apply it for each tick. This is useful for some computations like
   *                    sums where the only result that might matter is that of the latest tick
   * @param reversed Compute ticks backwards, starting from most recent one
   * @param step How many ticks to move the sliding window after each mapper application (>=1)
   * @param overrideTick If true, use the tick returned by the mapper instead of the current tick. This may lead to duplicate ticks, need to run DEDUP.
   *                    
   * @return A new GTS instance with the result of the Mapper.
   */
  public static List<GeoTimeSerie> map(GeoTimeSerie gts, WarpScriptMapperFunction mapper, long prewindow, long postwindow, long occurrences, boolean reversed, int step, boolean overrideTick) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, occurrences, reversed, step, overrideTick, null);
  }
  
  public static List<GeoTimeSerie> map(GeoTimeSerie gts, Object mapper, long prewindow, long postwindow, long occurrences, boolean reversed, int step, boolean overrideTick, WarpScriptStack stack) throws WarpScriptException {

    //
    // Make sure step is positive
    //
    
    if (step <= 0) {
      step = 1;
    }
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    //
    // Clone gts
    //
    
    GeoTimeSerie mapped = gts.clone();
    
    //
    // Do nothing if there are no values and gts was not bucketized
    //
    
    if (0 == mapped.values && !isBucketized(mapped)) {
      results.add(mapped);
      return results;
    }
    
    // Sort ticks
    sort(mapped, reversed);
    // Retrieve ticks if GTS is not bucketized.        
    long[] ticks = isBucketized(gts) ? null : Arrays.copyOf(mapped.ticks, gts.values);
    
    
    // Clear clone
    GTSHelper.clear(mapped);

    int idx = 0;
    // Number of ticks for which to run the mapper
    int nticks = null != ticks ? ticks.length : mapped.bucketcount;

    // Call getLabels once so we don't waste CPU cycles, this will create a clone of the labels map
    Map<String,String> labels = gts.getLabels();

    long tick = 0;
    
    GeoTimeSerie subgts = null;
    
    boolean hasOccurrences = (0 != occurrences);
    
    Map<String,GeoTimeSerie> multipleMapped = new TreeMap<String,GeoTimeSerie>();
    
    boolean hasSingleResult = false;
    
    while (idx < nticks) {

      if (hasOccurrences && 0 == occurrences) {
        break;
      }
      
      if (reversed) {
        tick = null != ticks ? ticks[idx] : mapped.lastbucket - idx * mapped.bucketspan;
      } else {
        tick = null != ticks ? ticks[idx] : mapped.lastbucket - (mapped.bucketcount - 1 - idx) * mapped.bucketspan;
      }
      
      //
      // Determine start/stop timestamp for extracting subserie
      //
      
      long start = tick;
      long stop = tick;
      
      if (prewindow < 0) {
        start = tick + prewindow;
      } else if (prewindow > 0) {
        // window is a number of ticks
        if (null == ticks) {
          start = prewindow <= mapped.bucketcount ? tick - prewindow * mapped.bucketspan : Long.MIN_VALUE;
        } else {
          if (reversed) {
            start = idx + prewindow < ticks.length ? (ticks[idx + (int) prewindow]) : Long.MIN_VALUE;
          } else {
            start = idx - prewindow >= 0 ? (ticks[idx - (int) prewindow]) : Long.MIN_VALUE;
          }
        }
      }
      
      if (postwindow < 0) {
        stop = tick - postwindow;
      } else if (postwindow > 0) {
        // window is a number of ticks
        if (null == ticks) {
          stop = postwindow <= mapped.bucketcount ? tick + postwindow * mapped.bucketspan : Long.MAX_VALUE;
        } else {
          if (reversed) {
            stop = idx - postwindow >= 0 ? (ticks[idx - (int) postwindow]) : Long.MAX_VALUE;
          } else {
            stop = idx + postwindow < ticks.length ? (ticks[idx + (int) postwindow]) : Long.MAX_VALUE;
          }
        }
      }
      
      //
      // Extract values 
      //
      
      subgts = GTSHelper.subSerie(gts, start, stop, false, false, subgts);
      
      Object mapResult = null;
      
      if (null != stack) {
        if (mapper instanceof Macro) {
          subgts.safeSetMetadata(mapped.getMetadata());
          stack.push(subgts);
          stack.exec((Macro) mapper);
          Object res = stack.peek();
          
          if (res instanceof List) {
            stack.drop();
            
            mapResult = MACROMAPPER.listToObjects((List) res);
          } else if (res instanceof Map) {
            stack.drop();
            
            Set<Object> keys = ((Map) res).keySet();
            
            for (Object key: keys) {
              Object[] ores2 = MACROMAPPER.listToObjects((List) ((Map) res).get(key));
              ((Map) res).put(key, ores2);
            }

            mapResult = res;
          } else {
            //
            // Retrieve result
            //

            mapResult = MACROMAPPER.stackToObjects(stack);
          }

        } else {
          throw new WarpScriptException("Invalid mapper function.");
        }        
      } else {
        if (!(mapper instanceof WarpScriptMapperFunction)) {
          throw new WarpScriptException("Expected a mapper function.");
        }
        //
        // Mapper functions have 8 parameters
        //
        // tick: timestamp we're computing the value for
        // names: array of names (for reducer compatibility)
        // labels: array of labels (for reducer compatibility)
        // ticks: array of ticks being aggregated
        // locations: array of locations being aggregated
        // elevations: array of elevations being aggregated
        // values: array of values being aggregated
        // window: An array with the window parameters [ prewindow, postwindow, start, stop, tick index ] on which the mapper runs
        //
        // 'window' nullity should be checked prior to using to allow mappers to be used as reducers.
        //
        // They return an array of 4 values:
        //
        // timestamp, location, elevation, value
        //
        // timestamp: an indication relative to timestamp (may be the timestamp at which the returned value was observed).
        //            it is usually not used (the returned value will be set at 'tick') but must be present.
        // location: location associated with the returned value
        // elevation: elevation associated with the returned value
        // value: computed value
        //
        
        Object[] parms = new Object[8];

        int i = 0;
        parms[i++] = tick;
        
        //
        // All arrays are allocated each time, so we don't risk
        // having a rogue mapper modify them.
        //
        
        parms[i++] = new String[subgts.values];
        Arrays.fill((Object[]) parms[i-1], gts.getName());

        parms[i++] = new Map[subgts.values]; 
        Arrays.fill((Object[]) parms[i-1], labels);

        parms[i++] = subgts.values > 0 ? Arrays.copyOf(subgts.ticks, subgts.values) : new long[0];
        if (null != subgts.locations) {
          parms[i++] = subgts.values > 0 ? Arrays.copyOf(subgts.locations, subgts.values) : new long[0];
        } else {
          if (subgts.values > 0) {
            parms[i++] = new long[subgts.values];
            Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
          } else {
            parms[i++] = new long[0];
          }
        }
        if (null != subgts.elevations) {
          parms[i++] = subgts.values > 0 ? Arrays.copyOf(subgts.elevations, subgts.values) : new long[0];
        } else {
          if (subgts.values > 0) {
            parms[i++] = new long[subgts.values];
            Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
          } else {
            parms[i++] = new long[0];
          }
        }
        parms[i++] = new Object[subgts.values];      

        int tickidx = -1;
        
        for (int j = 0; j < subgts.values; j++) {
          ((Object[]) parms[6])[j] = valueAtIndex(subgts, j);
          if (-1 == tickidx && tick == tickAtIndex(subgts, j)) {
            tickidx = j;
          }
        }

        parms[i++] = new long[] { prewindow, postwindow, start, stop, tickidx };
        
        mapResult = ((WarpScriptMapperFunction) mapper).apply(parms);
      }
            
      if (mapResult instanceof Map) {
        for (Entry<Object,Object> entry: ((Map<Object,Object>) mapResult).entrySet()) {
          GeoTimeSerie mgts = multipleMapped.get(entry.getKey().toString());
          if (null == mgts) {
            mgts = mapped.cloneEmpty();

            mgts.setName(entry.getKey().toString());
            multipleMapped.put(entry.getKey().toString(), mgts);
          }
          
          Object[] result = (Object[]) entry.getValue();
          if (null != result[3]) {
            GTSHelper.setValue(mgts, overrideTick ? (long) result[0] : tick, (long) result[1], (long) result[2], result[3], false);
          }
        }
      } else {
        Object[] result = mapResult instanceof List ? ((List) mapResult).toArray() : (Object[]) mapResult;
        
        //
        // Set value if it was not null. Don't overwrite, we scan ticks only once
        //

        hasSingleResult = true;
        
        if (null != result[3]) {
          GTSHelper.setValue(mapped, overrideTick ? (long) result[0] : tick, (long) result[1], (long) result[2], result[3], false);
        }        
      }
      
      idx += step;
      occurrences--;
    }

    if (hasSingleResult) {
      results.add(mapped);
    }
    
    if (!multipleMapped.isEmpty()) {
      results.addAll(multipleMapped.values());
    }
    
    return results;
  }
  
  public static List<GeoTimeSerie> map(GeoTimeSerie gts, WarpScriptMapperFunction mapper, long prewindow, long postwindow, long occurrences, boolean reversed) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, occurrences, reversed, 1, false);
  }
  
  public static List<GeoTimeSerie> map(GeoTimeSerie gts, WarpScriptMapperFunction mapper, long prewindow, long postwindow) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, 0, false);
  }
  
  /**
   * Modify the labels of a GTS instance.
   * If a label appears in 'newlabels', the associated value will be used in 'gts', unless
   * the associated value is the empty string or null in which case the said label will be removed
   * from 'gts'.
   * If a null key is present in the 'newlabels' map, the labels will replace those of 'gts'
   * instead of modifying them.
   * 
   * @param gts GTS instance whose labels must be modified.
   * @param newlabels Map of label names to label values.
   */
  public static GeoTimeSerie relabel(GeoTimeSerie gts, Map<String,String> newlabels) {
    Map<String,String> labels = new HashMap<String,String>();
    
    if (!newlabels.containsValue(null)) {
      labels.putAll(gts.getLabels());
    }
    
    for (String name: newlabels.keySet()) {
      String value = newlabels.get(name);
      if (null == value || "".equals(value)) {
        labels.remove(name);
        continue;
      }
    
      if (null != name) {
        labels.put(name, value);
      }
    }
    
    gts.setLabels(labels);
    
    return gts;
  }
  
  /**
   * Rename the given Geo Time Serie instance.
   * 
   * @param gts GTS instance to rename.
   * @param name New name to give the GTS, or suffix to add to its current name if 'name' starts with '+' (the '+' will be trimmed).
   */
  public static GeoTimeSerie rename(GeoTimeSerie gts, String name) {
    
    String newname = null;
    
    if (name.startsWith("+")) {
      newname = gts.getName() + name.substring(1); 
    } else {
      newname = name;
    }
    
    gts.setName(newname);
    
    gts.setRenamed(true);
    
    return gts;
  }
  
  /**
   * Partition a collection of Geo Time Serie instances into equivalence classes.
   * 
   * Member of each equivalence class have at least a common set of values for 'bylabels', they
   * may share common values for other labels too.
   * 
   * @param series Collection of GTS to partition
   * @param bylabels Collection of label names to use for partitioning or null to use all labels of each GTS
   * 
   * @return A map of label values to GTS instances
   */
  public static Map<Map<String,String>, List<GeoTimeSerie>> partition(Collection<GeoTimeSerie> series, Collection<String> bylabels) {
    
    Map<Map<String,String>, List<GeoTimeSerie>> classes = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
    Map<Map<String,String>, Map<String,String>> labelsbyclass = new HashMap<Map<String,String>, Map<String,String>>();
    
    //
    // Loop over the GTS instances
    //
    
    for (GeoTimeSerie gts: series) {
      //
      // Construct the equivalence class key
      //
      
      Map<String,String> eqcls = new HashMap<String,String>();

      //
      // If 'bylabels' is null, consider that all labels determine the equivalence class
      //
      
      if (null == bylabels) {
        eqcls.putAll(gts.getMetadata().getLabels());
      } else {
        for (String label: bylabels) {
          if (gts.hasLabel(label)) {
            eqcls.put(label, gts.getLabel(label));
          }
        }        
      }
            
      if(!classes.containsKey(eqcls)) {
        //
        // This equivalence class is not yet known, create an initial list of its members
        // and an initial Map of common labels
        //
        
        classes.put(eqcls, new ArrayList<GeoTimeSerie>());
        classes.get(eqcls).add(gts);
        labelsbyclass.put(eqcls, new HashMap<String,String>());
        labelsbyclass.get(eqcls).putAll(gts.getLabels());
      } else {
        //
        // Add current GTS to its class
        //
        
        classes.get(eqcls).add(gts);
        
        //
        // Remove from equivalence class labels those which 'gts' does not have
        //

        List<String> labelstoremove = new ArrayList<String>();
        
        Map<String,String> gtsLabels = gts.getMetadata().getLabels();
        
        for (String label: labelsbyclass.get(eqcls).keySet()) {
          if (!labelsbyclass.get(eqcls).get(label).equals(gtsLabels.get(label))) {
            labelstoremove.add(label);
          }
        }
        
        for (String label: labelstoremove) {
          labelsbyclass.get(eqcls).remove(label);
        }
      }
    }
    
    Map<Map<String,String>, List<GeoTimeSerie>> partition = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
    
    for (Map<String,String> key: classes.keySet()) {
      partition.put(labelsbyclass.get(key), classes.get(key));
    }
    return partition;
  }

  /**
   * Return the first tick in the GTS instance.
   * 
   * @param gts GeoTimeSerie to return the first tick for.
   * @return The first tick or Long.MAX_VALUE if 'gts' is not bucketized and has no values.
   */
  public static long firsttick(GeoTimeSerie gts) {
    if (isBucketized(gts)) {
      return gts.lastbucket - (gts.bucketcount - 1) * gts.bucketspan;
    } else {
      long firsttick = Long.MAX_VALUE;
      
      if (gts.sorted && gts.values > 0) {
        if (!gts.reversed) {
          firsttick = gts.ticks[0];
        } else {
          firsttick = gts.ticks[gts.values - 1];
        }
      } else {
        for (int i = 0; i < gts.values; i++) {
          if (gts.ticks[i] < firsttick) {
            firsttick = gts.ticks[i];
          }
        }        
      }
      
      return firsttick;
    }
  }

  /**
   * Return the last tick in the GTS instance.
   * 
   * @param gts GeoTimeSerie to return the last tick for.
   * 
   * @return The last tick or Long.MIN_VALUE if 'gts' is not bucketized and has no values.
   */
  public static long lasttick(GeoTimeSerie gts) {
    if (isBucketized(gts)) {
      return gts.lastbucket;
    } else {
      long lasttick = Long.MIN_VALUE;
      
      if (gts.sorted && gts.values > 0) {
        if (!gts.reversed) {
          lasttick = gts.ticks[gts.values - 1];
        } else {
          lasttick = gts.ticks[0];
        }
      } else {
        for (int i = 0; i < gts.values; i++) {
          if (gts.ticks[i] > lasttick) {
            lasttick = gts.ticks[i];
          }
        }
      }
      
      return lasttick;
    }
  }

  /**
   * Return the number of ticks in a GTS instance.
   * If the GTS is bucketized, the number of buckets is returned, not the number of actual values.
   * 
   * @param gts GeoTimeSerie instance of which to count ticks.
   * 
   * @return Number of ticks in GTS
   */
  public static int nticks(GeoTimeSerie gts) {
    if (isBucketized(gts)) {
      return gts.bucketcount;
    } else {
      return gts.values;
    }
  }
  
  /**
   * Return the number of values in a GTS instance.
   * 
   * @param gts GeoTimeSerit instance of which to count values.
   * 
   * @return Number of values in GTS.
   */
  
  public static int nvalues(GeoTimeSerie gts) {
    return gts.values;
  }
  
  /**
   * Handy method to compute the max value of a GTS instance.
   * 
   * It uses 'map_max' under the hood.
   * 
   * @param gts GTS to compute the max of
   * 
   * @return The computed max value
   */
  public static Object max(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[8];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = null;
    parms[i++] = null;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    parms[i++] = null;
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[6])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_MAX)).apply(parms);

    return result[3];
  }
  
  /**
   * Handy method to compute the min value of a GTS instance.
   * 
   * It uses 'map_min' under the hood.
   * 
   * @param gts GTS to compute the max of
   * 
   * @return The computed min value
   */
  public static Object min(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[8];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = null;
    parms[i++] = null;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    parms[i++] = null;
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[6])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_MIN)).apply(parms);

    return result[3];
  }

  /**
   * Handy method to compute the minimum elevation of a GTS instance.
   * 
   * It uses 'map_lowest' under the hood.
   * 
   * @param gts GTS to compute the minimum elevation on
   * 
   * @return The computed lowest elevation
   */
  public static Object lowest(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[6];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = 0L;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[5])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_LOWEST)).apply(parms);

    return result[3];
  }
  
  /**
   * Handy method to compute the maximum elevation of a GTS instance.
   * 
   * It uses 'map_highest' under the hood.
   * 
   * @param gts GTS to compute the maximum elevation on
   * 
   * @return The computed highest elevation
   */
  public static Object highest(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[6];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = 0L;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[5])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_HIGHEST)).apply(parms);

    return result[3];
  }
  
  public static long[] longValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.LONG != gts.type) {
      throw new WarpScriptException("Invalid type, expected LONG.");
    }
    
    return Arrays.copyOfRange(gts.longValues, 0, gts.values);
  }

  public static double[] doubleValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("Invalid type, expected DOUBLE.");
    }
    
    return Arrays.copyOfRange(gts.doubleValues, 0, gts.values);
  }

  public static BitSet booleanValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.BOOLEAN != gts.type) {
      throw new WarpScriptException("Invalid type, expected BOOLEAN.");
    }
    
    return gts.booleanValues.get(0, gts.values);
  }
  
  public static String[] stringValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.STRING != gts.type) {
      throw new WarpScriptException("Invalid type, expected STRING.");
    }
    
    return Arrays.copyOfRange(gts.stringValues, 0, gts.values);
  }
  
  /**
   * Remove duplicate ticks from the GeoTimeSerie instance.
   * 
   * Only the last value found for a given timestamp will be kept.
   * 
   * @param gts
   * @return
   */
  public static GeoTimeSerie dedup(GeoTimeSerie gts) {
    //
    // Start by cloning gts
    //
    
    GeoTimeSerie clone = gts.clone();
    
    //
    // If there is only one tick, there can't be duplicates
    //
    
    if (clone.values < 2) {
      return clone;
    }
    
    //
    // Now the real work begins...
    // We can't trivially sort 'clone' and scan its ticks
    // to remove duplicates, the dedup process retains
    // the LAST value encountered at a given tick, so we
    // can have compacted data AND individual measurements that
    // came afterwards and still retain the correct value
    //
    
    
    //
    // Extract and sort tick.
    //
    
    long[] ticks = Arrays.copyOf(clone.ticks, clone.values);
    
    Arrays.sort(ticks);
    
    //
    // Extract the set of duplicate ticks, maintaining a counter
    // of occurrences for each one
    //
    
    Map<Long,AtomicInteger> duplicates = new HashMap<Long,AtomicInteger>();
    
    int idx = 0;
    int idx2 = 1;
    
    while(idx2 < clone.values) {
      while(idx2 < clone.values && ticks[idx] == ticks[idx2]) {
        idx2++;
      }
      
      if (idx2 - idx > 1) {
        duplicates.put(ticks[idx], new AtomicInteger(idx2 - idx));
      }
      idx = idx2++;
    }
    
    //
    // If no duplicates were found, return 'clone' as is
    //

    if (0 == duplicates.size()) {
      return clone;
    }
    
    //
    // The duplicates map contains the timestamps of all duplicate ticks as keys and the number of
    // occurrences of each duplicate tick
    //
    
    // Index in the values array
    
    idx = 0;

    //
    // Loop over the original ticks/locations/elevations/values arrays and
    // if a tick is a duplicate and not the last occurrence of it, remove it
    // and decrement the duplicate count
    //
    
    idx = 0;
    int offset = 0;
      
    while(idx + offset < clone.values) {
      while (duplicates.containsKey(clone.ticks[idx+offset]) && duplicates.get(clone.ticks[idx+offset]).decrementAndGet() > 0) {
        offset++;
      }
      if (offset > 0) {
        clone.ticks[idx] = clone.ticks[idx + offset];
        if (null != clone.locations) {
          clone.locations[idx] = clone.locations[idx + offset];
        }
        if (null != clone.elevations) {
          clone.elevations[idx] = clone.elevations[idx + offset];
        }
        switch (clone.type) {
          case LONG:              
            clone.longValues[idx] = clone.longValues[idx + offset];
            break;
          case DOUBLE:
            clone.doubleValues[idx] = clone.doubleValues[idx + offset];
            break;
          case STRING:
            clone.stringValues[idx] = clone.stringValues[idx + offset];
            break;
          case BOOLEAN:
            clone.booleanValues.set(idx, clone.booleanValues.get(idx + offset));
            break;
        }          
      }
      idx++;
    }

    clone.values -= offset;

    //
    // If we removed more than 1000 duplicates, shrink GTS
    //
    
    if (offset > 1000) {
      GTSHelper.shrink(clone);
    }

    return clone;
  }
  
  /**
   * Removes non-bucket points of a buketized GTS
   * 
   * @param gts
   * @return
   */
  public static GeoTimeSerie onlybuckets(GeoTimeSerie gts) {
   
    GeoTimeSerie clone = gts.clone();
    
    // nothing to do if not bucketized
    if (!isBucketized(gts)) {
      return clone;
    }
    
    boolean setLocations = gts.locations != null;
    boolean setElevations = gts.elevations != null;
    
    int i = 0;
    while (i < clone.values) {
      
      // is current tick a bucket?
      long q = (gts.lastbucket - gts.ticks[i]) / gts.bucketspan;
      long r = (gts.lastbucket - gts.ticks[i]) % gts.bucketspan;
      if (0 == r && q >= 0 && q < gts.bucketcount) {
      
        // if yes then go to next tick
        i++;
      
        // else we will swap it with the bucket that has the biggest index
      } else {
        
        // searching this index and removing non-bucket ticks that have bigger index
        q = (gts.lastbucket - gts.ticks[clone.values - 1]) / gts.bucketspan;
        r = (gts.lastbucket - gts.ticks[clone.values - 1]) % gts.bucketspan;
        while (clone.values - 1 > i && !(0 == r && q >= 0 && q < gts.bucketcount)) {
          clone.values--;
          q = (gts.lastbucket - gts.ticks[clone.values - 1]) / gts.bucketspan;
          r = (gts.lastbucket - gts.ticks[clone.values - 1]) % gts.bucketspan;
        }
        
        if (clone.values - 1 == i) {
          // if this index does not exist, we just remove the last point
          clone.values--;
        } else {
        
          clone.ticks[i] = clone.ticks[clone.values - 1];
          if (setLocations) {
            clone.locations[i] = clone.locations[clone.values - 1];
          }
          if (setElevations) {
            clone.elevations[i] = clone.elevations[clone.values - 1];
          }
          
          switch(clone.type) {
            case LONG:
              clone.longValues[i] = clone.longValues[clone.values - 1];
              break;
            case DOUBLE:
              clone.doubleValues[i] = clone.doubleValues[clone.values - 1];
              break;
            case STRING:
              clone.stringValues[i] = clone.stringValues[clone.values - 1];
              break;
            case BOOLEAN:
              clone.booleanValues.set(i, clone.booleanValues.get(clone.values - 1));
              break;
          }
          
          // we remove the last point
          clone.values--;
          
          // then we go to the next tick
          i++;
        }        
      }
    }
    
    //
    // If we removed more than 1000 non-bucket ticks, shrink GTS
    //
    
    if (gts.values - clone.values > 1000) {
      GTSHelper.shrink(clone);
    }
    
    return clone;
  }
  
  /**
   * Apply a function (either a filter or n-ary op) onto collections of GTS instances.
   * 
   * Let's assume there are N collections in 'series'.
   *
   * The GTS instances from collections among those N that
   * have more than 1 element will be partitioned. For each
   * partition P, the function F will be called with arguments
   * 
   * F(labels, S(Ci,Pi), S(C1,P2), ... S(Ci,Pi), ...., S(CN,PN))
   * 
   * where S(Ci,Pi) is either the collection Ci if card(Ci) == 1
   * or only those series from Pi which belong to Ci.
   * 
   * 'labels' is the set of common labels in the partition.
   * 
   * @param function The function to apply, either an WarpScriptFilterFunction or 
   * @param bylabels Labels to use for partitioning the GTS instanes
   * @param series Set of GTS instances collections
   * @return
   * @throws WarpScriptException
   */
  public static List<GeoTimeSerie> partitionAndApply(Object function, WarpScriptStack stack, Macro validator, Collection<String> bylabels, List<GeoTimeSerie>... series) throws WarpScriptException {
    Map<Map<String,String>,List<GeoTimeSerie>> unflattened = partitionAndApplyUnflattened(function, stack, validator, bylabels, series);
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> l: unflattened.values()) {
      results.addAll(l);
    }
    
    return results;
  }
  
  /**
   * Apply a function or filter GTS and keep the results ventilated per equivalence class

   * @param function
   * @param bylabels
   * @param series
   * @return
   * @throws WarpScriptException
   */
  public static Map<Map<String,String>,List<GeoTimeSerie>> partitionAndApplyUnflattened(Object function, WarpScriptStack stack, Macro validator, Collection<String> bylabels, List<GeoTimeSerie>... series) throws WarpScriptException {

    //
    // Gather all GTS instances together so we can partition them
    // We omit the GTS instances which belong to collections with a single
    // GTS instance, instead those will systematically be added as parameters
    //
    //
    
    //Collection<GeoTimeSerie> allgts = new ArrayList<GeoTimeSerie>();
    Collection<GeoTimeSerie> allgts = new LinkedHashSet<GeoTimeSerie>();

    boolean hasNonSingleton = false;
    
    for (Collection<GeoTimeSerie> serie: series) {
      if (serie.size() > 1) {
        hasNonSingleton = true;
        allgts.addAll(serie);
      }
    }

    //
    // If all GTS lists are singletons, add the first GTS to allgts
    // so partition is not empty and thus the for loop does nothing.
    //
    
    if (!hasNonSingleton) {
      allgts.addAll(series[0]);
    }
    
    //
    // Partition the GTS instances
    //
    
    Map<Map<String,String>, List<GeoTimeSerie>> partition = GTSHelper.partition(allgts, bylabels);
    
    Map<Map<String,String>, List<GeoTimeSerie>> results = new LinkedHashMap<Map<String,String>,List<GeoTimeSerie>>();
    
    //
    // Loop on each partition
    //
    
    for (Map<String,String> partitionlabels: partition.keySet()) {
      Map<String,String> commonlabels = Collections.unmodifiableMap(partitionlabels);
      
      List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();

      //
      // Make N (cardinality of 'series') sublists of GTS instances.
      //
      
      List<GeoTimeSerie>[] subseries = new List[series.length];
      
      for (int i = 0; i < series.length; i++) {
        subseries[i] = new ArrayList<GeoTimeSerie>();
       
        //
        // Treat the case when the original serie had a cardinality of 1
        // as a special case by adding the original serie unconditionnaly
        //
        
        if (1 == series[i].size()) {
          subseries[i].add(series[i].iterator().next());
        } else {
          // The series appear in the order they are in the original list due to 'partition' using a List
          for (GeoTimeSerie serie: partition.get(partitionlabels)) {
            if (series[i].contains(serie)) {
              subseries[i].add(serie);
            }
          }          
        }
      }
      
      //
      // Call the function
      //
      
      if (function instanceof WarpScriptFilterFunction) {
        List<GeoTimeSerie> filtered = ((WarpScriptFilterFunction) function).filter(commonlabels, subseries);
        if (null != filtered) {
          result.addAll(filtered);
        }
      } else if (function instanceof WarpScriptNAryFunction) {
        //
        // If we have a stack and a validator, push the commonlabels and the list of subseries onto the stack,
        // call the validator and check if it left true or false onto the stack.
        //
        
        boolean proceed = true;
        
        if (null != stack && null != validator) {
          stack.push(Arrays.asList(subseries));
          stack.push(commonlabels);
          stack.exec(validator);
          if (!Boolean.TRUE.equals(stack.pop())) {
            proceed = false;
          }
        }
        
        if (proceed) {
          result.add(GTSHelper.applyNAryFunction((WarpScriptNAryFunction) function, commonlabels, subseries));
        }
      } else {
        throw new WarpScriptException("Invalid function to apply.");
      }
      
      results.put(commonlabels, result);
    }
    
    //
    // Check that all resulting GTS instances were in allgts
    //

    //if (function instanceof WarpScriptFilterFunction) {
    //  for (GeoTimeSerie gts: result) {
    //    if (!allgts.contains(gts)) {
    //      throw new WarpScriptException("Some filtered geo time series were not in the original set.");
    //    }
    //  }      
    //}
    
    return results;
  }
  
  public static GeoTimeSerie applyNAryFunction(WarpScriptNAryFunction function, Map<String,String> commonlabels, List<GeoTimeSerie>... subseries) throws WarpScriptException {
    
    commonlabels = Collections.unmodifiableMap(commonlabels);
    
    //
    // Determine if target should be bucketized.
    // Target will be bucketized if x and y have the same bucketspan,
    // and if their lastbucket values are congruent modulo bucketspan
    //
    
    long lastbucket = 0L;
    long firstbucket = Long.MAX_VALUE;
    long bucketspan = 0L;
    int bucketcount = 0;
    
    boolean done = false;
    
    for (int i = 0; i < subseries.length; i++) {
      for (GeoTimeSerie serie: subseries[i]) {
        // If we encountered a non bucketized GTS instance after
        // encountering a bucketized one, result won't be bucketized
        if (!isBucketized(serie) && bucketspan > 0) {
          done = true;
          break;
        }
        
        //
        // Skip the rest of the processing if GTS is not bucketized
        //
        
        if (!isBucketized(serie)) {
          continue;
        }
        
        //
        // If bucketspan differs from previous bucketspan, result
        // won't be bucketized
        //
        if (bucketspan > 0 && serie.bucketspan != bucketspan) {
          done = true;
          break;
        } else if (0L == bucketspan) {
          bucketspan = serie.bucketspan;
          lastbucket = serie.lastbucket;
        }
        
        //
        // If the lastbucket of this serie is not congruent to the
        // current lastbucket modulus 'bucketspan', result won't be
        // bucketized
        //
        
        if ((serie.lastbucket % bucketspan) != (lastbucket % bucketspan)) {
          done = true;
          break;
        }
        
        lastbucket = Math.max(serie.lastbucket, lastbucket);
        firstbucket = Math.min(firstbucket, serie.lastbucket - serie.bucketcount * bucketspan);
      }
      
      if (done) {
        break;
      }
    }

    //
    // We exited early or no GTS were bucketized, result won't be bucketized
    //
    
    if (done || 0 == bucketspan) {
      bucketspan = 0L;
      lastbucket = 0L;
      bucketcount = 0;
    } else {
      bucketcount = (int) ((lastbucket - firstbucket) / bucketspan);
    }

    //
    // Create target GTS
    //
    
    GeoTimeSerie gts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 16);
    gts.setName("");
    gts.setLabels(commonlabels);
    
    //
    // Sort all GTS instances so we can access their values cheaply
    // As GTS instances will be marked as sorted, sorting won't be done
    // multiple times
    //
    
    int idx[][] = new int[subseries.length][];
    int nseries = 0;
    
    // Keep a matrix with GTS labels, so we can avoid calling getLabels repeatedly
    Map<String,String>[][] partlabels = new Map[subseries.length][];
    
    for (int i = 0; i < subseries.length; i++) {
      idx[i] = new int[subseries[i].size()];
      partlabels[i] = new Map[subseries[i].size()];
      for (int j = 0; j < subseries[i].size(); j++) {
        partlabels[i][j] = subseries[i].get(j).getLabels();
        GTSHelper.sort(subseries[i].get(j));
        // Force name of target GTS, this is a brute force way to set it to the last name encountered...
        gts.setName(subseries[i].get(j).getName());
        nseries++;
      }
    }
    
    //
    // Allocate arrays
    //
    
    long[] ticks = new long[nseries];
    String[] names = new String[nseries];
    // Allocate one extra slot for common labels
    Map<String,String>[] labels = new Map[nseries + 1];
    long[] locations = new long[nseries];
    long[] elevations = new long[nseries];
    Object[] values = new Object[nseries];
    // Index of collection the value is from, if there is a gap in the index, a collection had no member in the partition
    // The last value is the initial number of collections
    int[] collections = new int[nseries + 1];
    collections[nseries] = subseries.length;
    
    Object[] params = new Object[8];
    
    // Do a sweeping line algorithm from oldest tick to newest
    
    while(true) {
      //
      // Determine the tick span at the given indices
      //

      long smallest = Long.MAX_VALUE;
      
      for (int i = 0; i < subseries.length; i++) {
        for (int j = 0; j < idx[i].length; j++) {
          GeoTimeSerie serie = subseries[i].get(j); 
          if (idx[i][j] < serie.values) {
            if (serie.ticks[idx[i][j]] < smallest) {
              smallest = serie.ticks[idx[i][j]];
            }
          }
        }
      }

      //
      // No smallest tick, this means we've exhausted all values
      //
      
      if (Long.MAX_VALUE == smallest) {
        break;
      }
           
      //
      // Now fill the locations/elevations/values arrays for all GTS
      // instances whose current tick is 'smallest'
      //
      
      int k = 0;
      
      for (int i = 0; i < subseries.length; i++) {
        for (int j = 0; j < idx[i].length; j++) {          
          GeoTimeSerie serie = subseries[i].get(j);
          
          names[k] = serie.getName();
          labels[k] = partlabels[i][j];
          
          // Tick records id of partition.
          ticks[k] = i;
          
          if (idx[i][j] < serie.values && smallest == serie.ticks[idx[i][j]]) {
            locations[k] = null != serie.locations ? serie.locations[idx[i][j]] : GeoTimeSerie.NO_LOCATION;
            elevations[k] = null != serie.elevations ? serie.elevations[idx[i][j]] : GeoTimeSerie.NO_ELEVATION;
            values[k] = GTSHelper.valueAtIndex(serie, idx[i][j]);
            idx[i][j]++;
          } else {
            locations[k] = GeoTimeSerie.NO_LOCATION;
            elevations[k] = GeoTimeSerie.NO_ELEVATION;
            values[k] = null;
          }
          collections[k] = i;
          k++;
        }
      }
    
      // Set common labels
      labels[k] = commonlabels;
      
      //
      // Call the reducer for the current tick
      //
      // Return value will be an array [tick, location, elevation, value]
      //
      
      params[0] = smallest;
      params[1] = names;
      params[2] = labels;
      params[3] = ticks;
      params[4] = locations;
      params[5] = elevations;
      params[6] = values;
      params[7] = collections;
      
      Object[] result = (Object[]) function.apply(params);

      if (null != result[3]) {
        GTSHelper.setValue(gts, smallest, (long) result[1], (long) result[2], result[3], false);
      }
    }

    return gts;
  }
  
  /**
   * Apply a binary op to pairs drawn from two collections of GTS instances.
   * 
   * If op1 has one element, op will be applied to (op1,y) where y belongs to op2. Result name and labels will be those of y.
   * If op2 has one element, op will be applied to (x,op2) where x belongs to op1. Result name and labels will be those of x.
   * 
   * If cardinalities of both op1 and op2 are > 1, the set {op1 + op2} will be partitioned using 'bylabels' (@see #partition).
   * If each partition contains 2 GTS instances, op will be applied on each partition. The result will have name and labels of the
   * partition element which belongs to op1.
   * 
   * @param op Binary op to apply.
   * @param op1 Collection of GTS instances for operand1 of op
   * @param op2 Collection of GTS instances for operand2 of op
   * @param bylabels Label names to use for partitioning op1+op2
   * 
   * @return A list of resulting GTS instances.
   * 
   * @throws WarpScriptException If partitioning could not be done
   */
  @Deprecated
  public static List<GeoTimeSerie> apply(WarpScriptBinaryOp op, Collection<GeoTimeSerie> op1, Collection<GeoTimeSerie> op2, Collection<String> bylabels) throws WarpScriptException {
    Collection<GeoTimeSerie> allgts = new ArrayList<GeoTimeSerie>();
    allgts.addAll(op1);
    allgts.addAll(op2);

    //
    // Handle the case when either op1 or op2 have a cardinality of 1, in which case
    // the semantics differ slightly, i.e. one GTS will be returned per one present in op1 or op2 (depending
    // which one has a single element).
    // In this case no partition is computed.
    //
    
    boolean oneToMany = false;
    
    if (1 == op1.size() || 1 == op2.size()) {
      oneToMany = true;
    }
    
    // FIXME(hbs): should we make sure op1 and op2 have no overlap
   
    //
    // Compute a partition of all GeoTimeSerie instances
    //
    
    Map<Map<String,String>, List<GeoTimeSerie>> partition = null;
    
    if (!oneToMany) {
      partition = partition(allgts, bylabels);
    } else {
      partition = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
      if (1 == op1.size()) {
        for (GeoTimeSerie gts: op2) {
          Map<String,String> labels = gts.getLabels();
          if (!partition.containsKey(labels)) {
            partition.put(labels, new ArrayList<GeoTimeSerie>());
            partition.get(labels).add(op1.iterator().next());
          }
          partition.get(labels).add(gts);
        }
      } else {
        for (GeoTimeSerie gts: op1) {
          Map<String,String> labels = gts.getLabels();
          if (!partition.containsKey(labels)) {
            partition.put(labels, new ArrayList<GeoTimeSerie>());
            partition.get(labels).add(op2.iterator().next());
          }
          partition.get(labels).add(gts);
        }        
      }
    }

    //
    // Check that each partition contains 2 elements, one from 'op1' and one from 'op2'
    //
    
    if (!oneToMany) {
      for (List<GeoTimeSerie> series: partition.values()) {
        if (2 != series.size()) {
          throw new WarpScriptException("Unable to partition operands coherently.");
        }
        if (!(op1.contains(series.get(0)) && op2.contains(series.get(1))) && !(op2.contains(series.get(0)) && op1.contains(series.get(1)))) {
          throw new WarpScriptException("Unable to partition operands coherently.");
        }
      }      
    }
        
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    //
    // For each partition, compute op(x,y) where x is in op1 and y in op2
    //
    
    if (!oneToMany) {
      for (Entry<Map<String,String>, List<GeoTimeSerie>> entry: partition.entrySet()) {
        
        List<GeoTimeSerie> series = entry.getValue();
        Map<String,String> commonlabels = ImmutableMap.copyOf(entry.getKey());
        
        //
        // Extract x and y
        //
        
        GeoTimeSerie x = op1.contains(series.get(0)) ? series.get(0) : series.get(1);
        GeoTimeSerie y = op2.contains(series.get(1)) ? series.get(1) : series.get(0);
        
        
        results.add(applyBinOp(op, x.getName(), commonlabels, x, y));
      }      
    } else {
      for (Entry<Map<String,String>, List<GeoTimeSerie>> entry: partition.entrySet()) {
        
        List<GeoTimeSerie> series = entry.getValue();
        Map<String,String> commonlabels = ImmutableMap.copyOf(entry.getKey());
        
        //
        // Extract x and y
        //
        
        GeoTimeSerie x = series.get(0);
        
        for (int i = 1; i < series.size(); i++) {
          GeoTimeSerie y = series.get(i);        
        
          if (op1.size() == 1) {
            results.add(applyBinOp(op, y.getName(), commonlabels, x, y));            
          } else {
            results.add(applyBinOp(op, y.getName(), commonlabels, y, x));
          }
        }
      }            
    }
    
    return results;
  }
  
  /**
   * Apply a binary op to two GTS instances
   * 
   * @param op Binary op to apply
   * @param name Name of resulting GTS
   * @param labels Labels of resulting GTS
   * @param x First operand GTS
   * @param y Second operand GTS
   * @return
   */
  private static GeoTimeSerie applyBinOp(WarpScriptBinaryOp op, String name, Map<String,String> labels, GeoTimeSerie x, GeoTimeSerie y) {
    //
    // Determine if target should be bucketized.
    // Target will be bucketized if x and y have the same bucketspan,
    // and if their lastbucket values are congruent modulo bucketspan
    //
    
    long lastbucket = 0L;
    long bucketspan = 0L;
    int bucketcount = 0;
    
    if (isBucketized(x) && isBucketized(y)) {
      if (x.bucketspan == y.bucketspan) {
        if ((x.lastbucket % x.bucketspan) == (y.lastbucket % y.bucketspan)) {
          lastbucket = Math.max(x.lastbucket, y.lastbucket);
          bucketspan = x.bucketspan;
          long firstbucket = Math.min(x.lastbucket - x.bucketcount * bucketspan, y.lastbucket - y.bucketcount * bucketspan);
          bucketcount = (int) ((lastbucket - firstbucket) / bucketspan);
        }
      }
    }
    
    //
    // Create target GTS
    //
    
    GeoTimeSerie gts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 16);
    gts.setName(name);
    gts.setLabels(labels);
    
    //
    // Sort x and y so we can scan the ticks cheaply
    //
    
    sort(x);
    sort(y);
    
    int xidx = 0;
    int yidx = 0;
    
    //
    // Binary ops have 8 parameters like mappers/reducers/aggregators
    //
    // tick -> the tick for which a value is computed
    // names -> array of GTS names (index 0 is x, 1 is y)
    // labels -> array of GTS labels (those from the original GTS) + 1 for the common labels (the last element of the array)
    // ticks -> array of ticks
    // locations -> array of locations
    // elevations -> array of elevations
    // values -> array of values
    //
    
    Object[] params = new Object[7];
    params[1] = new String[2];
    params[2] = new Map[3];
    params[3] = new long[2];
    params[4] = new long[2];
    params[5] = new long[2];
    params[6] = new Object[2];
    
    Map<String,String> xlabels = ImmutableMap.copyOf(x.getLabels());
    Map<String,String> ylabels = ImmutableMap.copyOf(y.getLabels());
          
    labels = ImmutableMap.copyOf(labels);
    
    
    // FIXME(hbs): should the type be determined by the first call with a non null value for X?
    
    while(xidx < x.values || yidx < y.values) {
      
      //
      //  Advance yidx until y tick catches x tick
      //
      
      while(yidx < y.values && x.ticks[xidx] >= y.ticks[yidx]) {
        
        long tick = y.ticks[yidx];
        Object xelt = x.ticks[xidx] == y.ticks[yidx] ? GTSHelper.valueAtIndex(x, xidx) : null;
        Object yelt = GTSHelper.valueAtIndex(y, yidx);
        
        params[0] = tick;
        ((String[]) params[1])[0] = x.getName();
        ((Map[]) params[2])[0] = xlabels;
        ((long[]) params[3])[0] = tick;
        
        if (null == xelt) {
          ((long[]) params[4])[0] = GeoTimeSerie.NO_LOCATION;
          ((long[]) params[5])[0] = GeoTimeSerie.NO_ELEVATION;
        } else {
          ((long[]) params[4])[0] = GTSHelper.locationAtIndex(x, xidx);
          ((long[]) params[5])[0] = GTSHelper.elevationAtIndex(x, xidx);
        }
        
        ((Object[]) params[6])[0] = xelt;            
        
        ((String[]) params[1])[1] = y.getName();
        ((Map[]) params[2])[1] = ylabels;
        ((long[]) params[3])[1] = tick;
        ((long[]) params[4])[1] = GTSHelper.locationAtIndex(y, yidx);
        ((long[]) params[5])[1] = GTSHelper.elevationAtIndex(y, yidx);
        ((Object[]) params[6])[1] = yelt;
        
        ((Map[]) params[2])[2] = labels;

        Object[] result = (Object[]) op.apply(params);

        Object value = result[3];
        
        if (null != value) {
          long location = (long) result[1];
          long elevation = (long) result[2];
          GTSHelper.setValue(gts, tick, location, elevation, value, false);
        }
        
        yidx++;
      }
    
      //
      // Advance the x index since the y tick is now > than the x tick
      //
      
      xidx++;
      
      while(xidx < x.values && ((yidx < y.values && x.ticks[xidx] < y.ticks[yidx]) || yidx >= y.values)) {

        long tick = x.ticks[xidx];
        Object xelt = x.ticks[xidx];
        // y has no elements since x tick is lagging behind y tick.
        Object yelt = null;
        
        params[0] = tick;
        ((String[]) params[1])[0] = x.getName();
        ((Map[]) params[2])[0] = xlabels;
        ((long[]) params[3])[0] = tick;
        
        if (null == xelt) {
          ((long[]) params[4])[0] = GeoTimeSerie.NO_LOCATION;
          ((long[]) params[5])[0] = GeoTimeSerie.NO_ELEVATION;
        } else {
          ((long[]) params[4])[0] = GTSHelper.locationAtIndex(x, xidx);
          ((long[]) params[5])[0] = GTSHelper.elevationAtIndex(x, xidx);
        }
        
        ((Object[]) params[6])[0] = xelt;            

        
        ((String[]) params[1])[1] = y.getName();
        ((Map[]) params[2])[1] = ylabels;
        ((long[]) params[3])[1] = tick;
        ((long[]) params[4])[1] = GeoTimeSerie.NO_LOCATION;
        ((long[]) params[5])[1] = GeoTimeSerie.NO_ELEVATION;
        ((Object[]) params[6])[1] = yelt; // null
        
        ((Map[]) params[2])[2] = labels;
        
        Object[] result = (Object[]) op.apply(params);

        Object value = result[3];
        
        if (null != value) {
          long location = (long) result[1];
          long elevation = (long) result[2];

          GTSHelper.setValue(gts, tick, location, elevation, value, false);
        }
        xidx++;
      }        
    }

    return gts;
  }
  
  public static List<GeoTimeSerie> reduce(WarpScriptReducerFunction reducer, Collection<GeoTimeSerie> series, Collection<String> bylabels) throws WarpScriptException {
    Map<Map<String,String>,List<GeoTimeSerie>> unflattened = reduceUnflattened(reducer, series, bylabels);
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> l: unflattened.values()) {
      results.addAll(l);
    }
    
    return results;
  }
  
  public static Map<Map<String,String>,List<GeoTimeSerie>> reduceUnflattened(WarpScriptReducerFunction reducer, Collection<GeoTimeSerie> series, Collection<String> bylabels) throws WarpScriptException {
    //
    // Partition the GTS instances using the given labels
    //
    
    Map<Map<String,String>, List<GeoTimeSerie>> partitions = partition(series, bylabels);
    
    Map<Map<String,String>,List<GeoTimeSerie>> results = new LinkedHashMap<Map<String,String>, List<GeoTimeSerie>>();
    
    
    for (Map<String,String> partitionLabels: partitions.keySet()) {
      boolean singleGTSResult = false;

      List<GeoTimeSerie> partitionSeries = partitions.get(partitionLabels);
      
      //
      // Extract labels and common labels
      //
      
      Map[] partlabels = new Map[partitionSeries.size() + 1];
      
      for (int i = 0; i < partitionSeries.size(); i++) {
        partlabels[i] = partitionSeries.get(i).getLabels();
      }
      
      partlabels[partitionSeries.size()] = Collections.unmodifiableMap(partitionLabels);
      
      //
      // Determine if result should be bucketized or not.
      // Result will be bucketized if all GTS instances in the partition are
      // bucketized, have the same bucketspan and have congruent lastbucket values
      //
      
      long endbucket = Long.MIN_VALUE;
      long startbucket = Long.MAX_VALUE;
      long lastbucket = Long.MIN_VALUE;
      long bucketspan = 0L;
      
      for (GeoTimeSerie gts: partitionSeries) {
        // One GTS instance is not bucketized, result won't be either
        if (!isBucketized(gts)) {
          bucketspan = 0L;          
          break;
        }
        if (0L == bucketspan) {
          bucketspan = gts.bucketspan;
        } else if (bucketspan != gts.bucketspan) {
          // GTS has a bucketspan which differs from the previous one,
          // so result won't be bucketized.
          bucketspan = 0L;
          break;
        }
        if (Long.MIN_VALUE == lastbucket) {
          lastbucket = gts.lastbucket;
        }
        if (lastbucket % bucketspan != gts.lastbucket % gts.bucketspan) {
          // GTS has a lastbucket value which is not congruent to the other
          // lastbucket values, so result GTS won't be bucketized.
          bucketspan = 0L;
          break;
        }
        //
        // Update start/end bucket
        //
        
        if (gts.lastbucket > endbucket) {
          endbucket = gts.lastbucket;
        }
        if (gts.lastbucket - gts.bucketcount * gts.bucketspan < startbucket) {
          startbucket = gts.lastbucket - gts.bucketcount * gts.bucketspan;
        }
      }
      
      //
      // Determine bucketcount if result is to be bucketized
      // startbucket is the end of the first bucket not considered
      //
      
      int bucketcount = 0;
      
      if (0L != bucketspan) {
        bucketcount = (int) ((endbucket - startbucket) / bucketspan);
      }
      
      //
      // Create target GTS
      //
      
      GeoTimeSerie result;
      
      if (0L != bucketspan) {
        result = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 0);
      } else {
        result = new GeoTimeSerie();
      }

      result.setLabels(partitionLabels);

      String name = null;

      //
      // Sort all series in the partition so we can scan their ticks in order
      // Checking if classname is identical between GTS. If so, using it
      //
        
      for (GeoTimeSerie gts: partitionSeries) {
        sort(gts, false);

        if (null == name){
          name = gts.getName();
        } else if (!gts.getName().equals(name) && name.length() != 0) {
          name = "";
        }
      }

      result.setName(name);

      
      Map<String,GeoTimeSerie> multipleResults = new TreeMap<String,GeoTimeSerie>();
      
      //
      // Initialize indices for each serie
      //
      
      int[] idx = new int[partitionSeries.size()];

      //
      // Initialize names/labels/location/elevation/value arrays
      //
      
      long[] ticks = new long[idx.length];
      String[] names = new String[idx.length];
      // Allocate 1 more slot for labels so we can store the common labels at the end of the array
      Map<String,String>[] lbls = Arrays.copyOf(partlabels, partlabels.length);
      
      long[] locations = new long[idx.length];
      long[] elevations = new long[idx.length];
      Object[] values = new Object[idx.length];
      
      //
      // Reducers have 7 parameters (similar to those of binary ops and mappers)
      //
      // tick for which value is computed
      // array of ticks
      // array of names
      // array of labels
      // array of locations
      // array of elevations
      // array of values
      //
      
      Object[] params = new Object[7];
      
      while(true) {
        //
        // Determine the tick span at the given indices
        //

        long smallest = Long.MAX_VALUE;
        
        for (int i = 0; i < idx.length; i++) {
          GeoTimeSerie gts = partitionSeries.get(i); 
          if (idx[i] < gts.values) {
            if (gts.ticks[idx[i]] < smallest) {
              smallest = gts.ticks[idx[i]];
            }
          }
        }

        //
        // No smallest tick, this means we've exhausted all values
        //
        
        if (Long.MAX_VALUE == smallest) {
          break;
        }
        
        //
        // Now fill the locations/elevations/values arrays for all GTS
        // instances whose current tick is 'smallest'
        //
        
        for (int i = 0; i < idx.length; i++) {
          GeoTimeSerie gts = partitionSeries.get(i); 
          if (idx[i] < gts.values && smallest == gts.ticks[idx[i]]) {
            ticks[i] = smallest;
            names[i] = gts.getName();
            //if (null == lbls[i]) {
            //  lbls[i] = gts.getLabels();
            //}
            locations[i] = null != gts.locations ? gts.locations[idx[i]] : GeoTimeSerie.NO_LOCATION;
            elevations[i] = null != gts.elevations ? gts.elevations[idx[i]] : GeoTimeSerie.NO_ELEVATION;
            values[i] = GTSHelper.valueAtIndex(gts, idx[i]);
            // Advance idx[i] since it was the smallest tick.
            idx[i]++;
          } else {
            ticks[i] = Long.MIN_VALUE;
            names[i] = gts.getName();
            //if (null == lbls[i]) {
            //  lbls[i] = gts.getLabels();
            //}
            locations[i] = GeoTimeSerie.NO_LOCATION;
            elevations[i] = GeoTimeSerie.NO_ELEVATION;
            values[i] = null;
          }
        }
        
        //
        // Call the reducer for the current tick
        //
        // Return value will be an array [tick, location, elevation, value]
        //
        
        // TODO(hbs): extend reducers to use a window instead of a single value when reducing.
        //            ticks/locations/elevations/values would be arrays of arrays and an 8th param
        //            could contain the values.
        
        params[0] = smallest;
        params[1] = names;
        params[2] = lbls;
        params[3] = ticks;
        params[4] = locations;
        params[5] = elevations;
        params[6] = values;
                
        Object reducerResult = reducer.apply(params);
        
        if (reducerResult instanceof Map) {
          for (Entry<Object,Object> entry: ((Map<Object,Object>) reducerResult).entrySet()) {
            GeoTimeSerie gts = multipleResults.get(entry.getKey().toString());
            if (null == gts) {
              if (0L != bucketspan) {
                gts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 0);
              } else {
                gts = new GeoTimeSerie();
              }

              gts.setName(entry.getKey().toString());
              gts.setLabels(partitionLabels);
              multipleResults.put(entry.getKey().toString(), gts);
            }
            
            Object[] reduced = (Object[]) entry.getValue();
            
            if (null != reduced[3]) {
              GTSHelper.setValue(gts, smallest, (long) reduced[1], (long) reduced[2], reduced[3], false);
            }
          }
        } else {
          Object[] reduced = (Object[]) reducerResult;
          singleGTSResult = true;
          if (null != reduced[3]) {
            GTSHelper.setValue(result, smallest, (long) reduced[1], (long) reduced[2], reduced[3], false);
          }
        }
        
      }
      
      if (!results.containsKey(partitionLabels)) {
        results.put(partitionLabels, new ArrayList<GeoTimeSerie>());
      }

      if (singleGTSResult) {
        results.get(partitionLabels).add(result);
      }

      if (!multipleResults.isEmpty()) {
        results.get(partitionLabels).addAll(multipleResults.values());
      }            
    }
    
    return results;
  }
  
  /**
   * Return the value of the most recent tick in this GTS.
   * 
   * @param gts GTS instance to extract value from.
   * 
   * @return
   */
  public static Object getLastValue(GeoTimeSerie gts) {
    
    // Easy one, if the GTS has no values then value is null
    
    if (0 == gts.values) {
      return null;
    }
    
    if (isBucketized(gts)) {
      for (int i = 0; i < gts.values; i++) {
        if (gts.lastbucket == gts.ticks[i]) {
          return GTSHelper.valueAtIndex(gts, i);
        }
      }
      return null;
    } else {
      long ts = Long.MIN_VALUE;
      int idx = -1;
      
      for (int i = 0; i < gts.values; i++) {
        if (gts.ticks[i] > ts) {
          ts = gts.ticks[i];
          idx = i;
        }
      }
      
      if (-1 != idx) {
        return GTSHelper.valueAtIndex(gts, idx);
      } else {
        return null;
      }
    }
  }
  
  /**
   * Return an array of indices sorted in the order of the matching value in the 'values' array.
   *  
   * @param values Values whose order must be checked
   * @param reversed If true, sort indices so 'values' is in descending order
   * 
   * @return 
   */
  public static int[] sortIndices(final long[] values, final boolean reversed) {
    Integer[] indices = new Integer[values.length];
    
    for (int i = 0; i < values.length; i++) {
      indices[i] = i;
    }
    
    Arrays.sort(indices, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        if (values[o1] - values[o1] < 0) {
          return reversed ? 1 : -1;
        } else if (values[o1] == values[o2]) {
          return 0;
        } else {
          return reversed ? -1 : 1;
        }
      }
    });
    
    int[] sorted = new int[indices.length];
    
    for (int i = 0; i < sorted.length; i++) {
      sorted[i] = indices[i];
    }
    
    return sorted;
  }
  
  public static TYPE getValueType(Object value) {
    if (value instanceof Long) {
      return TYPE.LONG;
    } else if (value instanceof Double) {
      return TYPE.DOUBLE;
    } else if (value instanceof String) {
      return TYPE.STRING;
    } else if (value instanceof Boolean) {
      return TYPE.BOOLEAN;
    } else {
      return TYPE.UNDEFINED;
    }
  }
  
  /**
   * Shrink the internal arrays of this GTS so their length matches
   * the number of values.
   * 
   * @param gts GTS instance to shrink
   */
  public static void shrink(GeoTimeSerie gts) {
    
    if (0 == gts.values) {
      gts.ticks = null;
      gts.locations = null;
      gts.elevations = null;
      gts.longValues = null;
      gts.doubleValues = null;
      gts.stringValues = null;
      gts.booleanValues = null;
      return;
    }
    
    if (null != gts.ticks && gts.ticks.length > gts.values) {
      gts.ticks = Arrays.copyOf(gts.ticks, gts.values);
    }
    if (null != gts.locations && gts.locations.length > gts.values) {
      gts.locations = Arrays.copyOf(gts.locations, gts.values);
    }
    if (null != gts.elevations && gts.elevations.length > gts.values) {
      gts.elevations = Arrays.copyOf(gts.elevations, gts.values);
    }
    switch(gts.type) {
      case UNDEFINED:
        gts.longValues = null;
        gts.doubleValues = null;
        gts.stringValues = null;
        gts.booleanValues = null;
        break;
      case LONG:
        gts.longValues = null != gts.longValues && gts.longValues.length > gts.values ? Arrays.copyOf(gts.longValues, gts.values) : gts.longValues;
        gts.doubleValues = null;
        gts.stringValues = null;
        gts.booleanValues = null;
        break;
      case DOUBLE:
        gts.longValues = null;
        gts.doubleValues = null != gts.doubleValues && gts.doubleValues.length > gts.values ? Arrays.copyOf(gts.doubleValues, gts.values) : gts.doubleValues;
        gts.stringValues = null;
        gts.booleanValues = null;
        break;
      case STRING:
        gts.longValues = null;
        gts.doubleValues = null;
        gts.stringValues = null != gts.stringValues && gts.stringValues.length > gts.values ? Arrays.copyOf(gts.stringValues, gts.values) : gts.stringValues;
        gts.booleanValues = null;
        break;
      case BOOLEAN:
        gts.longValues = null;
        gts.doubleValues = null;
        gts.stringValues = null;
        if (null != gts.booleanValues && gts.booleanValues.size() > gts.values) {
          BitSet newbits = new BitSet(gts.values);
          for (int i = 0; i < gts.values; i++) {
            newbits.set(i, gts.booleanValues.get(i));
          }
          gts.booleanValues = newbits;
        }
        break;
    }
  }
  
  /**
   * Compact a GeoTimeSerie instance by removing measurements which have the same
   * value/location/elevation as the previous one.
   * We retain the last value so the resulting GTS instance spans the same time
   * interval as the original one. Otherwise for example in the case of a constant
   * GTS, there would be only a single value which would means we would not know when
   * the last value was.
   * 
   * @param gts GTS instance to compress
   * @param preserveRanges Flag indicating if we should preserve range extrema or not
   * 
   * @return a new GTS instance which is a compressed version of the input one
   * 
   */
  public static GeoTimeSerie compact(GeoTimeSerie gts, boolean preserveRanges) {
    //
    // Clone gts
    //
    
    GeoTimeSerie clone = gts.clone();
    
    //
    // Sort so the ticks are in chronological order
    //
    
    GTSHelper.sort(clone);
    
    if (2 >= clone.values) {
      return clone;
    }
    
    //
    // Now scan the ticks and remove duplicate value/location/elevation tuples
    //
    
    int idx = 0;
    int offset = 0;
    // Start at index 1 so we keep the first value
    int compactIdx = 1;
    
    while(idx < clone.values - 1) {
      while(idx + 1 + offset < clone.values - 1
          && locationAtIndex(clone, idx + 1 + offset) == locationAtIndex(clone, idx)
          && elevationAtIndex(clone, idx + 1 + offset) == elevationAtIndex(clone, idx)
          && valueAtIndex(clone, idx + 1 + offset).equals(valueAtIndex(clone, idx))) {
        offset++;
      }
      
      //
      // Record the end of the range is preserveRanges is true
      //
      
      if (preserveRanges && offset > 0) {
        clone.ticks[compactIdx] = clone.ticks[idx + offset];
        if (null != clone.locations) {
          clone.locations[compactIdx] = clone.locations[idx + offset];
        }
        if (null != clone.elevations) {
          clone.elevations[compactIdx] = clone.elevations[idx + offset];
        }
        switch (clone.type) {
          case LONG:
            clone.longValues[compactIdx] = clone.longValues[idx + offset];
            break;
          case DOUBLE:
            clone.doubleValues[compactIdx] = clone.doubleValues[idx + offset];
            break;
          case BOOLEAN:
            clone.booleanValues.set(compactIdx, clone.booleanValues.get(idx + offset));
            break;
          case STRING:
            clone.stringValues[compactIdx] = clone.stringValues[idx + offset];
            break;
        }
        compactIdx++;
      }

      //
      // Record the new value
      //
      
      clone.ticks[compactIdx] = clone.ticks[idx + offset + 1];
      if (null != clone.locations) {
        clone.locations[compactIdx] = clone.locations[idx + offset + 1];
      }
      if (null != clone.elevations) {
        clone.elevations[compactIdx] = clone.elevations[idx + offset + 1];
      }
      switch (clone.type) {
        case LONG:
          clone.longValues[compactIdx] = clone.longValues[idx + offset + 1];
          break;
        case DOUBLE:
          clone.doubleValues[compactIdx] = clone.doubleValues[idx + offset + 1];
          break;
        case BOOLEAN:
          clone.booleanValues.set(compactIdx, clone.booleanValues.get(idx + offset + 1));
          break;
        case STRING:
          clone.stringValues[compactIdx] = clone.stringValues[idx + offset + 1];
          break;
      }
        
      //
      // Advance indices
      //
      idx = idx + offset + 1;
      compactIdx++;
      offset = 0;
    }

    //
    // Copy the last value
    //
    
    if (offset > 0) {
      clone.ticks[clone.values - offset - 1] = clone.ticks[clone.values - 1];
      if (null != clone.locations) {
        clone.locations[clone.values - offset - 1] = clone.locations[clone.values - 1];
      }
      if (null != clone.elevations) {
        clone.elevations[clone.values - offset - 1] = clone.elevations[clone.values - 1];
      }
      switch (clone.type) {
        case LONG:
          clone.longValues[clone.values - offset - 1] = clone.longValues[clone.values - 1];
          break;
        case DOUBLE:
          clone.doubleValues[clone.values - offset - 1] = clone.doubleValues[clone.values - 1];
          break;
        case BOOLEAN:
          clone.booleanValues.set(clone.values - offset - 1, clone.booleanValues.get(clone.values - 1));
          break;
        case STRING:
          clone.stringValues[clone.values - offset - 1] = clone.stringValues[clone.values - 1];
          break;
      }
    }
    clone.values = compactIdx;
    
    GTSHelper.shrink(clone);
    
    return clone;
  }
  
  /**
   * Normalize a GTS, replacing X by (X-MIN)/(MAX-MIN) or 1.0
   * @param gts
   * @return
   */
  public static GeoTimeSerie normalize(GeoTimeSerie gts) {
    //
    // Return immediately if GTS is not numeric or has no values
    //
    if ((TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) || 0 == gts.values) {
      return gts.clone();
    }

    //
    // Extract min/max
    //
    
    double dmin = Double.POSITIVE_INFINITY;
    double dmax = Double.NEGATIVE_INFINITY;
    
    long lmin = Long.MAX_VALUE;
    long lmax = Long.MIN_VALUE;
    
    if (TYPE.LONG == gts.getType()) {
      for (int i = 0; i < gts.values; i++) {
        long value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue();
        
        if (value > lmax) {
          lmax = value;
        }
        if (value < lmin) {
          lmin = value;
        }
      }      
    } else {
      for (int i = 0; i < gts.values; i++) {
        double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
        
        if (value > dmax) {
          dmax = value;
        }
        if (value < dmin) {
          dmin = value;
        }
      }
    }
    
    boolean constant = false;
    
    if (lmin == lmax || dmin == dmax) {
      constant = true;
    }
    
    //
    // Don't use clone or cloneEmpty, this would force the type to that of 'gts'
    
    GeoTimeSerie normalized = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    normalized.setMetadata(new Metadata(gts.getMetadata()));
    
    for (int i = 0; i < gts.values; i++) {
      Object value;
      
      if (constant) {
        value = 1.0D;
      } else if (TYPE.LONG == gts.getType()) {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).longValue() - lmin) / (double) (lmax - lmin);
      } else {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - dmin) / (double) (dmax - dmin);
      }
      
      GTSHelper.setValue(normalized, gts.ticks[i], GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
    }
    
    return normalized;
  }

  /**
   * Normalize a GTS, replacing X by (X-MEAN)/(MAX-MIN) or 1.0
   * @param gts
   * @return
   */
  public static GeoTimeSerie isonormalize(GeoTimeSerie gts) {
    //
    // Return immediately if GTS is not numeric or has no values
    //
    if ((TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) || 0 == gts.values) {
      return gts.clone();
    }

    //
    // Extract min/max
    //
    
    double sum = 0.0D;
    
    double dmin = Double.POSITIVE_INFINITY;
    double dmax = Double.NEGATIVE_INFINITY;
    
    long lmin = Long.MAX_VALUE;
    long lmax = Long.MIN_VALUE;
    
    if (TYPE.LONG == gts.getType()) {
      for (int i = 0; i < gts.values; i++) {
        long value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue();
        
        if (value > lmax) {
          lmax = value;
        }
        if (value < lmin) {
          lmin = value;
        }
        
        sum += value;
      }      
    } else {
      for (int i = 0; i < gts.values; i++) {
        double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
        
        if (value > dmax) {
          dmax = value;
        }
        if (value < dmin) {
          dmin = value;
        }
        
        sum += value;
      }
    }
    
    boolean constant = false;
    
    if (lmin == lmax || dmin == dmax) {
      constant = true;
    }
    
    //
    // Don't use clone or cloneEmpty, this would force the type to that of 'gts'
    
    GeoTimeSerie isonormalized = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    isonormalized.setMetadata(new Metadata(gts.getMetadata()));
    
    double mean = sum / gts.values;
    
    for (int i = 0; i < gts.values; i++) {
      Object value;
      
      if (constant) {
        value = 1.0D;
      } else if (TYPE.LONG == gts.getType()) {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).longValue() - mean) / (double) (lmax - lmin);
      } else {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - mean) / (double) (dmax - dmin);
      }
      
      GTSHelper.setValue(isonormalized, gts.ticks[i], GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
    }
    
    return isonormalized;
  }

  /**
   * Standardize a numeric GeoTimeSerie
   * 
   * @param gts
   * @return
   */
  public static GeoTimeSerie standardize(GeoTimeSerie gts) {
    /**
     * Return immediately if GTS is not numeric or has no values
     */
    if ((TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) || 0 == gts.values) {
      return gts.clone();
    }
    
    //
    // Compute sum of values and sum of squares
    //
    
    double sum = 0.0D;
    double sumsq = 0.0D;
    
    for (int i = 0; i < gts.values; i++) {
      double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
      sum += value;
      sumsq += value * value;
    }
    
    //
    // Compute mean and standard deviation
    //
    
    double mean = sum / (double) gts.values;
    
    double variance = (sumsq / (double) gts.values) - (sum * sum) / ((double) gts.values * (double) gts.values);
    
    //
    // Apply Bessel's correction
    // @see http://en.wikipedia.org/wiki/Bessel's_correction
    //
    
    if (gts.values > 1) {
      variance = variance * ((double) gts.values) / (gts.values - 1.0D);
    }

    double sd = Math.sqrt(variance);
    
    GeoTimeSerie standardized = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    standardized.setMetadata(new Metadata(gts.getMetadata()));

    for (int i = 0; i < gts.values; i++) {
      double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
      // Subtract mean
      value = value - mean;
      // Divide by sd if sd is not null
      if (0.0D != sd) {
        value = value / sd;
      }
      GTSHelper.setValue(standardized, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, value, false);
    }
    
    return standardized;
  }
  
  /**
   * Produces a String GTS whose values are the encoded bSAX words of the sequence starting at each tick.
   * 
   * It only works on numeric bucketized GTS instances.
   * 
   * @param gts
   * @param alphabetSize
   * @param wordLen
   * @param windowLen
   * @param standardizePAA
   * @return
   */
  public static GeoTimeSerie bSAX(GeoTimeSerie gts, int alphabetSize, int wordLen, int windowLen, boolean standardizePAA) throws WarpScriptException {
    
    if (!GTSHelper.isBucketized(gts) || (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type)) {
      throw new WarpScriptException("Function can only be applied to numeric, bucketized, filled geo time series.");
    }
    
    if (windowLen % wordLen != 0) {
      throw new WarpScriptException("Wordlen MUST divide windowlen.");
    }
    
    //
    // Check if alphabetSize is a power of 2
    //
    
    int levels = 0;
    
    if (0 == alphabetSize) {
      throw new WarpScriptException("Alphabet size MUST be a power of two.");      
    }
    
    while(0 == (alphabetSize & 1)) {
      levels++;
      alphabetSize >>>= 1;
    }
    
    if (1 != alphabetSize) {
      throw new WarpScriptException("Alphabet size MUST be a power of two.");      
    }

    if (levels < 1 || levels > SAXUtils.SAX_MAX_LEVELS) {
      throw new WarpScriptException("Alphabet size MUST be a power of two between 2 and 2^" + SAXUtils.SAX_MAX_LEVELS);
    }

    // Compute number of values to aggregate using PAA to obtain the correct number of sax symbols per word given the sliding window length
    int paaLen = windowLen / wordLen;
 
    //
    // Sort GTS
    //
    
    GTSHelper.sort(gts);
    
    GeoTimeSerie saxGTS = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    saxGTS.setMetadata(gts.getMetadata());
    
    int[] symbols = new int[wordLen];
    double paaSum[] = new double[wordLen];
    
    for (int i = 0; i < gts.values - windowLen + 1; i++) {
      //
      // Apply PAA
      //

      double sum = 0.0D;
      double sumsq = 0.0D;
      
      
      for (int w = 0; w < wordLen; w++) {
        paaSum[w] = 0.0D;
        
        for (int k = 0; k < paaLen; k++) {
          paaSum[w] += TYPE.LONG == gts.type ? gts.longValues[i + w * paaLen + k] : gts.doubleValues[i + w * paaLen + k];
        }
              
        if (!standardizePAA) {
          continue;
        }

        double mean = paaSum[w] / paaLen;
        sum += mean;
        sumsq += mean * mean;
        //sum += paaSum[w];
        //sumsq += paaSum[w] * paaSum[w];        
      }
      
      //
      // Normalize window
      //
      
      double mu = 0.0D;
      
      double variance = 0.0D;
      double sigma = 0.0D;
      
      if (standardizePAA) {
        mu = sum / wordLen;
        variance = (sumsq / wordLen) - (sum * sum) / ((double) wordLen * (double) wordLen);
        //
        // Apply Bessel's correction
        // @see http://en.wikipedia.org/wiki/Bessel's_correction
        //
        
        if (wordLen > 1) {
          variance = variance * wordLen / (wordLen - 1.0D);
        }
        
        sigma =  Math.sqrt(variance);
      }
      
      for (int w = 0; w < wordLen; w++) {
        // Compute value to use for generating SAX symbol
        if (standardizePAA) {
          symbols[w] = SAXUtils.SAX(levels, sigma != 0D ? ((paaSum[w] / paaLen) - mu) / sigma : ((paaSum[w] / paaLen) - mu));
        } else {
          symbols[w] = SAXUtils.SAX(levels, paaSum[w] / paaLen);         
        }
      }

      //
      // Generate bSAX words
      //
      
      String word = new String(OrderPreservingBase64.encode(SAXUtils.bSAX(levels, symbols)), Charsets.US_ASCII);
      
      GTSHelper.setValue(saxGTS, gts.ticks[i], word);      
    }
    
    return saxGTS;
  }
  
  /**
   * Perform exponential smoothing on a numeric GTS.
   * 
   * @param gts
   * @param alpha Smoothing Factor (0 < alpha < 1)
   * @return
   */
  public static GeoTimeSerie singleExponentialSmoothing(GeoTimeSerie gts, double alpha) throws WarpScriptException {
    
    //
    // Alpha must be between 0 and 1
    //
    
    if (alpha <= 0.0D || alpha >= 1.0D) {
      throw new WarpScriptException("The smoothing factor must be in 0 < alpha < 1.");
    }
    
    //
    // Exponential smoothing only works on numeric GTS with at least two values
    //
    
    if (TYPE.LONG != gts.type && TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("Can only perform exponential smoothing on numeric geo time series.");
    }
    
    if (gts.values < 2) {
      throw new WarpScriptException("Can only perform exponential smoothing on geo time series containing at least two values.");
    }
    
    GeoTimeSerie s = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    s.setMetadata(new Metadata(gts.getMetadata()));
    
    //
    // Sort input GTS
    //
    
    GTSHelper.sort(gts);
    
    double smoothed = ((Number) GTSHelper.valueAtIndex(gts, 0)).doubleValue();
    double oneminusalpha = 1.0D - alpha;
    
    GTSHelper.setValue(s, gts.ticks[0], null != gts.locations ? gts.locations[0] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[0] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
    
    for (int i = 1; i < gts.values; i++) {
      smoothed = alpha * ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() + oneminusalpha * smoothed;
      
      GTSHelper.setValue(s, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
    }
    
    return s;
  }
  
  /**
   * Perform a double exponential smoothing on the input GTS.
   * 
   * @param gts
   * @param alpha
   * @param beta
   * @return
   * @throws WarpScriptException
   */
  public static List<GeoTimeSerie> doubleExponentialSmoothing(GeoTimeSerie gts, double alpha, double beta) throws WarpScriptException {
    
    //
    // Alpha and Beta must be between 0 and 1
    //
    
    if (alpha <= 0.0D || alpha >= 1.0D) {
      throw new WarpScriptException("The data smoothing factor must be in 0 < alpha < 1.");
    }

    if (beta <= 0.0D || beta >= 1.0D) {
      throw new WarpScriptException("The trend smoothing factor must be in 0 < beta < 1.");
    }

    //
    // Exponential smoothing only works on numeric GTS with at least two values
    //
    
    if (TYPE.LONG != gts.type && TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("Can only perform exponential smoothing on numeric geo time series.");
    }
    
    if (gts.values < 2) {
      throw new WarpScriptException("Can only perform exponential smoothing on geo time series containing at least two values.");
    }

    //
    // Allocate a list for returning smoothed GTS and best estimate GTS
    //
    
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    // Smoothed GTS
    GeoTimeSerie s = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    s.setMetadata(new Metadata(gts.getMetadata()));

    // Best estimate GTS
    GeoTimeSerie b = s.clone();
    
    GTSHelper.sort(gts);
    
    double smoothed = ((Number) GTSHelper.valueAtIndex(gts, 1)).doubleValue();
    double bestestimate = smoothed - ((Number) GTSHelper.valueAtIndex(gts, 0)).doubleValue();
    double oneminusalpha = 1.0D - alpha;
    double oneminusbeta = 1.0D - beta;
        
    GTSHelper.setValue(s, gts.ticks[1], null != gts.locations ? gts.locations[1] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[1] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
    GTSHelper.setValue(b, gts.ticks[1], null != gts.locations ? gts.locations[1] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[1] : GeoTimeSerie.NO_ELEVATION, bestestimate, false);

    for (int i = 2; i < gts.values; i++) {
      double newsmoothed = alpha * ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() + oneminusalpha * (smoothed + bestestimate);
      bestestimate = beta * (newsmoothed - smoothed) + oneminusbeta * bestestimate;
      smoothed = newsmoothed;
      GTSHelper.setValue(s, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
      GTSHelper.setValue(b, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, bestestimate, false);
    }
    
    result.add(s);
    result.add(b);
    
    return result;
  }
  
  /**
   * Build an occurrence count by value for the given time serie.
   */
  public static Map<Object,Long> valueHistogram(GeoTimeSerie gts) {
    //
    // Sort gts
    //
    
    Map<Object, Long> occurrences = new HashMap<Object, Long>();
    
    //
    // Count the actual values
    //
    
    for (int i = 0; i < gts.values; i++) {
      Object value = GTSHelper.valueAtIndex(gts, i);
      
      if (!occurrences.containsKey(value)) {
        occurrences.put(value, 1L);
      } else {        
        occurrences.put(value, 1L + occurrences.get(value));        
      }
    }
    
    //
    // If the GTS is bucketized, add the count of empty values
    //
    
    if (GTSHelper.isBucketized(gts) && gts.bucketcount != gts.values) {
      occurrences.put(null, (long) (gts.bucketcount - gts.values));
    }
    
    return occurrences;
  }
  
  /**
   * Detect patterns in a Geo Time Serie instance. Return a modified version of the original
   * GTS instance where only the values which are part of one of the provided patterns are kept.
   * 
   * @param gts
   * @param alphabetSize
   * @param wordLen
   * @param windowLen
   * @param standardizePAA
   * @return
   */
  public static GeoTimeSerie detect(GeoTimeSerie gts, int alphabetSize, int wordLen, int windowLen, Collection<String> patterns, boolean standardizePAA) throws WarpScriptException {
    //
    // Generate patterns for the provided GTS
    //
    
    GeoTimeSerie gtsPatterns = GTSHelper.bSAX(gts, alphabetSize, wordLen, windowLen, standardizePAA);
    
    //
    // Sort gtsPatterns
    //
    
    GTSHelper.sort(gtsPatterns);
    
    //
    // Scan the ticks, 
    //
    
    GeoTimeSerie detected = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, 16);
    detected.setMetadata(gts.getMetadata());
    
    // Last index included, used to speed up things
    int lastidx = -1;
    
    for (int i = 0; i < gtsPatterns.values; i++) {
      if (!patterns.contains(gtsPatterns.stringValues[i])) {
        continue;
      }
      
      //
      // The pattern at the current tick is one we want to detect, include 'windowLen' ticks into the 'detected' GTS
      //
      
      for (int j = 0; j < windowLen; j++) {
        if (i + j > lastidx) {
          lastidx = i + j;
          GTSHelper.setValue(detected, GTSHelper.tickAtIndex(gts, lastidx), GTSHelper.locationAtIndex(gts, lastidx), GTSHelper.elevationAtIndex(gts, lastidx), GTSHelper.valueAtIndex(gts, lastidx), false);
        }
      }
    }
    
    return detected;
  }
  
  public static int getBucketCount(GeoTimeSerie gts) {
    return gts.bucketcount;
  }
  
  public static long getBucketSpan(GeoTimeSerie gts) {
    return gts.bucketspan;
  }
  
  public static long getLastBucket(GeoTimeSerie gts) {
    return gts.lastbucket;
  }

  public static void setBucketCount(GeoTimeSerie gts, int bucketcount) {
    gts.bucketcount = bucketcount;
  }
  
  public static void setBucketSpan(GeoTimeSerie gts, long bucketspan) {
    gts.bucketspan = bucketspan;
  }
  
  public static void setLastBucket(GeoTimeSerie gts, long lastbucket) {
    gts.lastbucket = lastbucket;
  }

  public static void metadataToString(StringBuilder sb, String name, Map<String,String> labels) {
    GTSHelper.encodeName(sb, name);
    
    labelsToString(sb, labels);
  }
  
  public static void labelsToString(StringBuilder sb, Map<String,String> labels) {
    sb.append("{");
    boolean first = true;
    
    if (null != labels) {
      for (Entry<String, String> entry: labels.entrySet()) {
        //
        // Skip owner/producer labels and any other 'private' labels
        //
        if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
          continue;
        }
        if (Constants.OWNER_LABEL.equals(entry.getKey())) {
          continue;
        }
        
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }      
    }
    
    sb.append("}");
  }
  
  public static String buildSelector(GeoTimeSerie gts) {
    return buildSelector(gts.getMetadata());
  }
  
  public static String buildSelector(Metadata metadata) {
    StringBuilder sb = new StringBuilder();
    encodeName(sb, metadata.getName());
    sb.append("{");
    TreeMap<String,String> labels = new TreeMap<String,String>(metadata.getLabels());
    boolean first = true;
    for (Entry<String,String> entry: labels.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      encodeName(sb, entry.getKey());
      sb.append("=");
      encodeName(sb,entry.getValue());
      first = false;
    }
    sb.append("}");
    
    return sb.toString();
  }
  
  /**
   * Return a GTS which is a subset of the input GTS with only those ticks which
   * fall between start and end (both inclusive).
   * 
   * @param gts
   * @param start
   * @param end
   * @return
   */
  public static GeoTimeSerie timeclip(GeoTimeSerie gts, long start, long end) {
    GeoTimeSerie clipped = gts.cloneEmpty();
    
    for (int idx = 0; idx < gts.values; idx++) {
      long ts = GTSHelper.tickAtIndex(gts, idx);
      
      if (ts < start || ts > end) {
        if (gts.sorted) {
          if ((gts.reversed && ts < start) || (!gts.reversed && ts > end)) {
            break;
          }
        }
        continue;     
      }
      
      GTSHelper.setValue(clipped, ts, GTSHelper.locationAtIndex(gts, idx), GTSHelper.elevationAtIndex(gts, idx), GTSHelper.valueAtIndex(gts, idx), false);
    }
    
    return clipped;
  }
  
  /**
   * 'Integrate' a GTS, considering the value at each tick is a rate of change per second.
   * 
   * @param gts
   * @param initialValue
   * @return
   */
  public static GeoTimeSerie integrate(GeoTimeSerie gts, double initialValue) {
    GeoTimeSerie integrated = gts.cloneEmpty();
    
    //
    // Sort GTS so ticks are in ascending order
    //
    
    GTSHelper.sort(gts);
    
    double value = initialValue;
    
    GTSHelper.setValue(integrated, GTSHelper.tickAtIndex(gts, 0), GTSHelper.locationAtIndex(gts, 0), GTSHelper.elevationAtIndex(gts,  0), value, false);
    
    for (int i = 1; i < gts.values; i++) {
      double deltaT = (double) (gts.ticks[i] - gts.ticks[i - 1]) / (double) Constants.TIME_UNITS_PER_S;
      
      double rateOfChange = ((Number) GTSHelper.valueAtIndex(gts, i - 1)).doubleValue();
      
      value = value + rateOfChange * deltaT;
      
      GTSHelper.setValue(integrated, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts,  i), value, false);
    }
    
    return integrated;
  }
  
  /**
   * Reduce the number of values in a GTS.
   * 
   * @param gts Geo Time Series to reduce
   * @param newsize New number of values
   * @return
   */
  public static GeoTimeSerie shrinkTo(GeoTimeSerie gts, int newsize) throws WarpScriptException {
    if (newsize > 0 && newsize < gts.values) {
      gts.values = newsize;
    }
    
    return gts;
  }
  
  /**
   * Splits a GTS in 'chunks' of equal time length.
   * 
   * @param gts
   * @param lastchunk End timestamp of the most recent chunk. Use 0 to adjust automatically on 'chunkwidth' boundary
   * @param chunkwidth Width of each chunk in time units
   * @param chunkcount Number of chunks to generate. Use 0 to generate as many chunks as needed to cover the GTS
   * @param chunklabel Name of the label to use for storing the chunkid
   * @param keepempty Should we keed empty chunks
   * @return
   * @throws WarpScriptException
   */
  public static List<GeoTimeSerie> chunk(GeoTimeSerie gts, long lastchunk, long chunkwidth, long chunkcount, String chunklabel, boolean keepempty) throws WarpScriptException {
    return chunk(gts, lastchunk, chunkwidth, chunkcount, chunklabel, keepempty, 0L);    
  }
  
  public static List<GeoTimeSerie> chunk(GeoTimeSerie gts, long lastchunk, long chunkwidth, long chunkcount, String chunklabel, boolean keepempty, long overlap) throws WarpScriptException {
    
    if (overlap < 0 || overlap > chunkwidth) {
      throw new WarpScriptException("Overlap cannot exceed chunk width.");
    }
    
    //
    // Check if 'chunklabel' exists in the GTS labels
    //
    
    Metadata metadata = gts.getMetadata();
    
    if(metadata.getLabels().containsKey(chunklabel)) {
      throw new WarpScriptException("Cannot operate on Geo Time Series which already have a label named '" + chunklabel + "'");
    }

    TreeMap<Long, GeoTimeSerie> chunks = new TreeMap<Long,GeoTimeSerie>();
    
    //
    // If GTS is bucketized, make sure bucketspan is less than boxwidth
    //
    
    boolean bucketized = GTSHelper.isBucketized(gts);
    
    if (bucketized) {
      if (gts.bucketspan > chunkwidth) {
        throw new WarpScriptException("Cannot operate on Geo Time Series with a bucketspan greater than the chunk width.");
      }
    } else {
      // GTS is not bucketized and has 0 values, if lastchunk was 0, return an empty list as we
      // are unable to produce chunks
      if (0 == gts.values && 0L == lastchunk) {
        return new ArrayList<GeoTimeSerie>();
      }
    }
    
    //
    // Set chunkcount to Integer.MAX_VALUE if it's 0
    //
    
    boolean zeroChunkCount = false;
    
    if (0 == chunkcount) {
      chunkcount = Integer.MAX_VALUE;
      zeroChunkCount = true;
    }
    
    //
    // Sort timestamps in reverse order so we can produce all chunks in O(n)
    //
    
    GTSHelper.sort(gts, true);

    //
    // Loop on the chunks
    //
    
    // Index in the timestamp array
    int idx = 0;

    long bucketspan = gts.bucketspan;
    int bucketcount = gts.bucketcount;
    long lastbucket = gts.lastbucket;
    
    //
    // If lastchunk is 0, use lastbucket or the most recent tick
    //
    
    if (0 == lastchunk) {
      if (isBucketized(gts)) {
        lastchunk = lastbucket;
      } else {
        // Use the most recent tick
        lastchunk = gts.ticks[0]; 
        // Make sure lastchunk is aligned on 'chunkwidth' boundary
        if (0 != (lastchunk % chunkwidth)) {
          lastchunk = lastchunk - (lastchunk % chunkwidth) + chunkwidth;
        }
      }            
    }

    //
    // Compute size hints so allocations are faster.
    // We heuristically consider that each chunk will contain an equal share of
    // the original points. We do the same for the overlaps.
    //
    
    int hint = (int) (chunkcount > 0 ? gts.values / chunkcount : 0);
    int overlaphint = (int) (overlap > 0 ? gts.values / overlap : 0);
    
    //
    // The final hint takes intoaccount the overlaps
    //
    
    if (hint > 0) {
      hint += 2 * overlaphint;
    }
    
    for (long i = 0; i < chunkcount; i++) {

      //System.out.println("CHUNK=" + i + " HINT=" + hint);
      
      // If we have no more values and were not specified a chunk count, exit the loop, we're done
      if (idx >= gts.values && zeroChunkCount) {
        break;
      }
      
      // Compute chunk bounds
      long chunkend = lastchunk - i * chunkwidth;
      long chunkstart = chunkend - chunkwidth + 1;

      GeoTimeSerie chunkgts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, hint);
      
      // Set metadata for the GTS
      chunkgts.setMetadata(metadata);
     // Add 'chunklabel'
      chunkgts.getMetadata().putToLabels(chunklabel, Long.toString(chunkend));
            
      if (bucketized) {
        // Chunk is outside the GTS, it will be empty 
        if (lastbucket < chunkstart || chunkend <= lastbucket - (bucketcount * bucketspan)) {
          // Add the (empty) chunk if keepempty is true
          if (keepempty || overlap > 0) {
            chunks.put(chunkend,chunkgts);
          }
          continue;
        }

        // Set the bucketized parameters in the GTS
        
        // If bucketspan does not divide chunkwidth, chunks won't be bucketized
        
        if (0 == chunkwidth % bucketspan) {
          chunkgts.bucketspan = bucketspan;
          chunkgts.lastbucket = chunkend;
          chunkgts.bucketcount = (int) ((chunkend - chunkstart + 1) / bucketspan);
        } else {
          chunkgts.bucketspan = 0L;
          chunkgts.lastbucket = 0L;
          chunkgts.bucketspan = 0;
        }
      }
      
      //
      // Add the datapoints which fall within the current chunk
      //
      
      // Advance until the current tick is before 'chunkend'       
      while (idx < gts.values && gts.ticks[idx] > chunkend) {
        idx++;
      }

      // We've exhausted the values
      if (idx >= gts.values) {
        // only add chunk if it's not empty or empty with 'keepempty' set to true
        if (0 != chunkgts.values || (keepempty || overlap > 0)) {
          chunks.put(chunkend, chunkgts);
        }
        continue;
      }
      
      // The current tick is before the beginning of the current chunk
      if (gts.ticks[idx] < chunkstart) {
        // only add chunk if it's not empty or empty with 'keepempty' set to true
        if (0 != chunkgts.values || (keepempty || overlap > 0)) {
          chunks.put(chunkend, chunkgts);
        }
        continue;
      }
      
      while(idx < gts.values && gts.ticks[idx] >= chunkstart) {
        GTSHelper.setValue(chunkgts, GTSHelper.tickAtIndex(gts, idx), GTSHelper.locationAtIndex(gts, idx), GTSHelper.elevationAtIndex(gts, idx), GTSHelper.valueAtIndex(gts, idx), false);
        idx++;
      }
      
      // only add chunk if it's not empty or empty with 'keepempty' set to true
      if (0 != chunkgts.values || (keepempty || overlap > 0)) {
        chunks.put(chunkend,chunkgts);
      }
      
      //
      // If there is no overlap and the chunk has empty slots, shrink it now so we save memory.
      // If using overlap and the chunk has more empty array slots than 2 * overlaphint, shrink it.
      // This will slow down the overlap computation but will save memory.
      //
      
      if (0 == chunkgts.values) {
        GTSHelper.shrink(chunkgts);
      } else if (overlap <= 0 && gts.values > 0 && chunkgts.ticks.length - chunkgts.values > 0) {
        GTSHelper.shrink(chunkgts);
      } else if (overlap > 0 && chunkgts.values > 0 && chunkgts.ticks.length - chunkgts.values > 2 * overlaphint) {
        GTSHelper.shrink(chunkgts);
      }
      
      //
      // Adapt the hint
      //
      
      if (chunkgts.values > hint) {
        hint = chunkgts.values + 2 * overlaphint;
      } else if (chunkgts.values < hint) {
        hint = ((hint + chunkgts.values) / 2) + 2 * overlaphint;
      }
    }
    
    //
    // Handle overlapping is need be.
    // We need to iterate over all ticks and add datapoints to each GTS they belong to
    //
    
    if (overlap > 0) {
      
      //
      // Check if we need to add a first and a last chunk
      //
      
      long ts = GTSHelper.tickAtIndex(gts, 0);
      
      if (ts <= chunks.firstKey() - chunkwidth) {
        Entry<Long,GeoTimeSerie> currentFirst = chunks.firstEntry();
        GeoTimeSerie firstChunk = currentFirst.getValue().cloneEmpty();
        if (GTSHelper.isBucketized(currentFirst.getValue())) {
          firstChunk.lastbucket = firstChunk.lastbucket - firstChunk.bucketspan;
        }
        chunks.put(currentFirst.getKey() - chunkwidth, firstChunk);
      }
      
      ts = GTSHelper.tickAtIndex(gts, gts.values - 1);
      
      if (ts >= chunks.lastKey() - chunkwidth + 1 - overlap) {
        Entry<Long,GeoTimeSerie> currentLast = chunks.lastEntry();
        GeoTimeSerie lastChunk = currentLast.getValue().cloneEmpty();
        if (GTSHelper.isBucketized(currentLast.getValue())) {
          lastChunk.lastbucket = lastChunk.lastbucket + lastChunk.bucketspan;
        }
        chunks.put(currentLast.getKey() + chunkwidth, lastChunk);
      }
      
      //
      // Put all entries in a list so we can access them randomly
      //
      
      List<Entry<Long,GeoTimeSerie>> allchunks = new ArrayList<Entry<Long,GeoTimeSerie>>(chunks.entrySet());

      int[] currentSizes = new int[allchunks.size()];
            
      for (int i = 0; i < currentSizes.length; i++) {
        currentSizes[i] = allchunks.get(i).getValue().values;
      }
      
      //
      // Iterate over chunks, completing with prev and next overlaps
      // Remember the timestamps are in reverse order so far.
      //
      
      for (int i = 0; i < allchunks.size(); i++) {
        GeoTimeSerie current = allchunks.get(i).getValue();
        long lowerBound = allchunks.get(i).getKey() - chunkwidth + 1 - overlap;
        long upperBound = allchunks.get(i).getKey() + overlap;
        if (i > 0) {
          GeoTimeSerie prev = allchunks.get(i - 1).getValue();
          for (int j = 0; j < currentSizes[i - 1]; j++) {
            long timestamp = GTSHelper.tickAtIndex(prev, j);
            if (timestamp < lowerBound) {
              break;
            }
            GTSHelper.setValue(current, timestamp, GTSHelper.locationAtIndex(prev, j), GTSHelper.elevationAtIndex(prev, j), GTSHelper.valueAtIndex(prev, j), false);
          }
        }
        if (i < allchunks.size() - 1) {
          GeoTimeSerie next = allchunks.get(i + 1).getValue();
          for (int j = currentSizes[i + 1] - 1; j >=0; j--) {
            long timestamp = GTSHelper.tickAtIndex(next, j);
            if (timestamp > upperBound) {
              break;
            }
            GTSHelper.setValue(current, timestamp, GTSHelper.locationAtIndex(next, j), GTSHelper.elevationAtIndex(next, j), GTSHelper.valueAtIndex(next, j), false);
          }
        }
      }
    }
    
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    for (GeoTimeSerie g: chunks.values()) {
      if (!keepempty && 0 == g.values) {
        continue;
      }
      
      //
      // Shrink the GTS if the underlying storage is unused for more than 10% of the capacity
      //
      
      if (g.values > 0 && g.ticks.length - g.values > g.values * 0.1) {
        GTSHelper.shrink(g);
      }
      
      result.add(g);
    }

    return result;
  }
  
  public static GeoTimeSerie fuse(Collection<GeoTimeSerie> chunks) throws WarpScriptException {
    //
    // Check if all chunks are of the same type
    //

    if (!chunks.isEmpty()) {

      TYPE type = null;

      int size = 0;

      for (GeoTimeSerie chunk : chunks) {
        // Set the type of the result from the type of the first chunk with
        // a defined type.
        if (null == type) {
          if (TYPE.UNDEFINED != chunk.type) {
            type = chunk.type;
          }
          continue;
        }
        if (0 != chunk.values && type != chunk.type) {
          throw new WarpScriptException("Inconsistent types for chunks to fuse.");
        }
        size += chunk.values;
      }

      //
      // Determine if we have compatible bucketization parameters.
      // bucketspan should be the same for all chunks and lastbucket should
      // be congruent to the same value modulo bucketspan
      //

      long lastbucket = Long.MIN_VALUE;
      long bucketspan = 0L;
      long firstbucket = Long.MAX_VALUE;

      for (GeoTimeSerie chunk : chunks) {
        // If one chunk is not bucketized, exit
        if (!isBucketized(chunk)) {
          bucketspan = 0L;
          break;
        }
        if (0L == bucketspan) {
          bucketspan = chunk.bucketspan;
          lastbucket = chunk.lastbucket;
          firstbucket = lastbucket - bucketspan * (chunk.bucketcount - 1);
        } else {
          // If bucketspan is not the same as the previous one, exit, result won't be bucketized
          if (bucketspan != chunk.bucketspan) {
            bucketspan = 0L;
            break;
          }
          // Check if lastbucket and chunk.lastbucket are congruent to the same value modulo the bucketspan, if not result is not bucketized
          if ((lastbucket % bucketspan) != (chunk.lastbucket % bucketspan)) {
            bucketspan = 0L;
            break;
          }
          // Update lastbucket and firstbucket
          if (chunk.lastbucket > lastbucket) {
            lastbucket = chunk.lastbucket;
          }
          if (chunk.lastbucket - (chunk.bucketcount - 1) * chunk.bucketspan < firstbucket) {
            firstbucket = chunk.lastbucket - (chunk.bucketcount - 1) * chunk.bucketspan;
          }
        }
      }

      int bucketcount = 0;

      if (0L == bucketspan) {
        lastbucket = 0L;
      } else {
        // Compute bucketcount
        bucketcount = (int) (1 + ((lastbucket - firstbucket) / bucketspan));
      }

      // Create the fused GTS
      GeoTimeSerie fused = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, size);

      // Merge the datapoints and jointly determine class and labels

      String classname = null;
      boolean hasClass = false;
      Map<String, String> labels = null;

      for (GeoTimeSerie chunk : chunks) {

        if (null == classname) {
          classname = chunk.getMetadata().getName();
          hasClass = true;
        } else if (!classname.equals(chunk.getMetadata().getName())) {
          hasClass = false;
        }

        Map<String, String> chunklabels = chunk.getMetadata().getLabels();

        if (null == labels) {
          labels = new HashMap<String, String>();
          labels.putAll(chunklabels);
        } else {
          // Determine the common labels of chunks
          for (Entry<String, String> entry : chunklabels.entrySet()) {
            if (!entry.getValue().equals(labels.get(entry.getKey()))) {
              labels.remove(entry.getKey());
            }
          }
        }

        for (int i = 0; i < chunk.values; i++) {
          setValue(fused, GTSHelper.tickAtIndex(chunk, i), GTSHelper.locationAtIndex(chunk, i), GTSHelper.elevationAtIndex(chunk, i), GTSHelper.valueAtIndex(chunk, i), false);
        }
      }

      //
      // Set labels and class
      //

      if (hasClass) {
        fused.setName(classname);
      }

      fused.setLabels(labels);

      return fused;

    }

    return new GeoTimeSerie();
  }
  
  public static GeoTimeSerie timescale(GeoTimeSerie gts, double scale) throws WarpScriptException {
    GeoTimeSerie scaled = gts.clone();
    
    for (int i = 0; i < scaled.values; i++) {
      scaled.ticks[i] = (long) (scaled.ticks[i] * scale);
    }
    
    scaled.lastbucket = (long) (scaled.lastbucket * scale);        
    scaled.bucketspan = (long) (scaled.bucketspan * scale);

    if (gts.bucketspan != 0 && 0 == scaled.bucketspan) {
      throw new WarpScriptException("Down scaling is too agressive, bucketspan would end up 0.");
    }
    return scaled;
  }
  
  /**
   * Determine if a GTS' values are normally distributed.
   * Works for numerical GTS only

   * @param gts GTS to check
   * @param buckets Number of buckets to distribute the values in
   * @param pcterror Maximum acceptable percentage of deviation from the mean of the bucket sizes
   * @param bessel Should we apply Bessel's correction when computing sigma
   * 
   * @return true if the GTS is normally distributed (no bucket over pcterror from the mean bucket size)
   */
  public static boolean isNormal(GeoTimeSerie gts, int buckets, double pcterror, boolean bessel) {
    //
    // Return true if GTS has no values
    //
    
    if (0 == gts.values) {
      return true;
    }
    
    //
    // Return false if GTS is not of type LONG or DOUBLE
    //
    
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      return false;
    }
    
    double[] musigma = musigma(gts, bessel);
    double mu = musigma[0];
    double sigma = musigma[1];
    
    //
    // Constant GTS are not gaussian
    //
    
    if (0.0D == sigma) {
      return false;
    }
    
    //
    // Retrieve bounds
    //
    
    double[] bounds = SAXUtils.getBounds(buckets);
    
    int[] counts = new int[bounds.length + 1];
    
    //
    // Loop over the values, counting the number of occurrences in each interval
    //
    
    for (int i = 0; i < gts.values; i++) {
      double v = (((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - mu) / sigma;
      
      int insertion = Arrays.binarySearch(bounds, v);
      
      //(-(insertion_point) - 1);
      
      if (insertion >= 0) {
        counts[insertion]++;
      } else {
        counts[-(1 + insertion)]++;
      }
    }
    
    //
    // Compute the mean bucket size
    //
    
    double mean = gts.values / counts.length;

    //
    // Check that each bucket is with 'pcterror' of the mean
    //
    
    for (int i = 0; i < counts.length; i++) {
      if (Math.abs(1.0D - (counts[i] / mean)) > pcterror) {
        return false;
      }
    }
    
    return true;
  }
  
  public static double[] musigma(GeoTimeSerie gts, boolean bessel) {
    //
    // Compute mu and sigma
    //
    
    double sum = 0.0D;
    double sumsq = 0.0D;
    
    double[] musigma = new double[2];
    
    if (TYPE.DOUBLE == gts.type) {
      for (int i = 0; i < gts.values; i++) {
        sum += gts.doubleValues[i];
        sumsq += gts.doubleValues[i] * gts.doubleValues[i];
      }
    } else {
      for (int i = 0; i < gts.values; i++) {
        double v = (double) gts.longValues[i]; 
        sum += v;
        sumsq += v * v;
      }      
    }
    
    musigma[0] = sum / gts.values;
    double variance = (sumsq / gts.values) - (sum * sum / ((double) gts.values * (double) gts.values));
    
    if (bessel && gts.values > 1) {
      variance = variance * gts.values / ((double) gts.values - 1.0D);
    }
    
    musigma[1] = Math.sqrt(variance);
    
    return musigma;
  }
  
  public static GeoTimeSerie quantize(GeoTimeSerie gts, double[] bounds, Object[] values) throws WarpScriptException {
    
    // FIXME(hbs): we could support String types too
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      throw new WarpScriptException("Can only quantify numeric Geo Time Series.");
    }
    
    GeoTimeSerie quantified = gts.cloneEmpty();
    
    //
    // Loop over the values, counting the number of occurrences in each interval
    //
    
    for (int i = 0; i < gts.values; i++) {
      double v = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
      
      int insertion = Arrays.binarySearch(bounds, v);
      
      //(-(insertion_point) - 1);
      
      if (null == values) {
        if (insertion >= 0) {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), insertion, false);
        } else {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), -(1 + insertion), false);
        }        
      } else {
        if (insertion >= 0) {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), values[insertion], false);
        } else {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), values[-(1 + insertion)], false);
        }
      }
    }

    return quantified;
  }
  
  /**
   * Return an array with a copy of the ticks of the given GTS.
   * 
   * @param gts
   * @return
   */
  public static long[] getTicks(GeoTimeSerie gts) {
    return Arrays.copyOf(gts.ticks, gts.values);
  }
  
  /**
   * Return an array with a copy of the locations of the given GTS.
   * Allocation a new array if no location was specified in the GTS.
   * 
   * @param gts
   * @return
   */
  public static long[] getLocations(GeoTimeSerie gts) {
    if (null != gts.locations) {
      return Arrays.copyOf(gts.locations, gts.values);
    } else {
      long[] locations = new long[gts.values];
      Arrays.fill(locations, GeoTimeSerie.NO_LOCATION);
      return locations;
    }
  }
  
  public static long[] getOriginalLocations(GeoTimeSerie gts) {
    return gts.locations;
  }
  
  public static long[] getOriginalElevations(GeoTimeSerie gts) {
    return gts.elevations;
  }

  /**
   * Return an array with a copy of the locations of the given GTS.
   * Allocation a new array if no location was specified in the GTS.
   * 
   * @param gts
   * @return
   */
  public static long[] getElevations(GeoTimeSerie gts) {
    if (null != gts.elevations) {
      return Arrays.copyOf(gts.elevations, gts.values);
    } else {
      long[] elevations = new long[gts.values];
      Arrays.fill(elevations, GeoTimeSerie.NO_ELEVATION);
      return elevations;
    }
  }
  
  /**
   * Return an array with a copy of the GTS values as doubles
   * 
   * @param gts
   * @return
   * @throws WarpScriptException
   */
  public static double[] getValuesAsDouble(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.DOUBLE == gts.type) {
      return Arrays.copyOf(gts.doubleValues, gts.values);
    } else if (TYPE.LONG == gts.type) {
      double[] values = new double[gts.values];
      for (int i = 0; i < gts.values; i++) {
        values[i] = gts.longValues[i];
      }
      return values;
    } else {
      throw new WarpScriptException("Invalid Geo Time Series type.");
    }
  }

  /**
   * Transform a Metadata instance by 'intern'ing all of its strings.
   * 
   * @param meta
   */
  public static void internalizeStrings(Metadata meta) {
    String name = meta.getName();
    
    if (null != name) {
      meta.setName(name.intern());
    }

    //
    // The approach we take is to create a new instance of Map, filling
    // it with the interned keys and values and replaceing labels/attributes.
    //
    // This approach is safe as we do not modify the map underlying the entrySet
    // we iterate over.
    // 
    // Modifying the original map while itearating over it is prone to throwing ConcurrentModificationException
    // in the case when a bin contains multiple nodes since we MUST do a remove then a put to intern both
    // the key AND the value.
    //
    // The ideal solution would be to have a modified HashMap which exposes an 'internalize' method which would
    // call intern for every String in keys and values
    //
    
    if (meta.getLabelsSize() > 0) {
      Map<String,String> newlabels = new HashMap<String, String>();
      for (Entry<String,String> entry: meta.getLabels().entrySet()) {
        String key = entry.getKey().intern();
        String value = entry.getValue().intern();
        newlabels.put(key, value);
      }
      meta.setLabels(newlabels);
    }
    
    if (meta.getAttributesSize() > 0) {
      Map<String,String> newattributes = new HashMap<String, String>();
      for (Entry<String,String> entry: meta.getAttributes().entrySet()) {
        String key = entry.getKey().intern();
        String value = entry.getValue().intern();
        newattributes.put(key, value);
      }
      meta.setAttributes(newattributes);
    }
  }
  
  /**
   * Compute local weighted regression at given tick
   * 
   * @param gts      : input GTS
   * @param idx      : considered as index of the first non-null neighbour at the right
   * @param tick     : tick at which lowess is achieved
   * @param q        : bandwitdth, i.e. number of nearest neighbours to consider
   * @param d        : degree of polynomial fit
   * @param weights  : optional array that store the weights
   * @param rho      : optional array that store the robustness weights
   * @param beta     : optional array that store the regression parameters
   * @param reversed : should idx be considered to be at the left instead
   */
  public static double pointwise_lowess(GeoTimeSerie gts, int idx, long tick, int q, int p, double[] weights, double[] rho, double[] beta, boolean reversed) throws WarpScriptException {
    
    if (null != weights && q > weights.length || (null != rho && gts.values > rho.length) || (null != beta && p + 1 > beta.length) ) {
      throw new WarpScriptException("Incoherent array lengths as input of pointwise_lowess");
    }
    /**
     * FIXME(JCV):
     * q = 3: 22 errors out of 100 points (at these points the value is almost equal to the value at q=2)
     * q = 4: 2 errors out of 100 points (at these points the value is almost equal to the value at q=3)
     * other q: 0 error
     * But it's not meant to be used with low value of q anyway.
     */
    
    //
    // Determine the 'q' closest values
    // We use two indices i and j for that, i moves forward, j moves backward from idx, until we
    // identified 'q' values (or 'n' if q > n)
    //
    
    int i = idx;
    int j = idx - 1;
    
    if (reversed) {
      i += 1;
      j += 1;
    }
    
    
    int count = 0;
    
    while(count < q) {
      long idist = Long.MAX_VALUE;
      long jdist = Long.MAX_VALUE;
      
      if (i < gts.values) {
        idist = Math.abs(tickAtIndex(gts, i) - tick);
      }
      
      if (j >= 0) {
        jdist = Math.abs(tickAtIndex(gts, j) - tick);
      }
      
      // If we exhausted the possible values
      if (Long.MAX_VALUE == idist && Long.MAX_VALUE == jdist) {
        break;
      }
      
      if (idist < jdist) {
        i++;
      } else {
        j--;
      }
      
      count++;
    }

    // The 'q' nearest values are between indices j and i (excluded)
    
    // Compute the maximum distance from 'tick'
    
    double maxdist = Math.max(j < -1 ? 0.0D : Math.abs(tickAtIndex(gts, j + 1) - tick), i <= 0 ? 0.0D : Math.abs(tickAtIndex(gts, i - 1) - tick));

    // Adjust maxdist if q > gtq.values
    
    if (q > gts.values) {
      maxdist = (maxdist * q) / gts.values;
    }
          
    // Compute the weights
    
    // Reset the weights array
    if (null == weights) {
      weights = new double[q];
    } else {
      Arrays.fill(weights, 0.0D);
    }
      
    int widx = 0;
    
    double wsum = 0.0D;
    
    for (int k = j + 1; k < i; k++) {
      if (0 == maxdist) {
        weights[widx] = 1.0D;
      } else {         
        double u = Math.abs(gts.ticks[k] - tick) / maxdist;
        
        if (u >= 1.0) {
          weights[widx] = 0.0D;
        } else {
          
          weights[widx] = 1.0D - u * u * u;
          
          double rho_ = 1.0D;
          if (null != rho) {
            // In some cases, "all rho are equal to 0.0", which should be translated to "all rho are equal" (or else there is no regression at all)
            // So if rho equals zero we set it to a certain value which is low enough not to bias the result in case all rho are not all equals.
            rho_ = 0.0D != rho[k] ? rho[k] : 0.000001D;
          }          
          weights[widx] = rho_ * weights[widx] * weights[widx] * weights[widx];
        }
      }
      wsum += weights[widx];
      widx++;
    }
    
    // Regression parameters
    if (null == beta) {
      beta = new double[p + 1];
    }

    //
    // Linear polynomial fit
    //
    
    if (1 == p){
      
      //
      // Compute weighted centroids for ticks and values
      //
      
      widx = 0;
      
      double ctick = 0.0D;
      double cvalue = 0.0D;
          
      for (int k = j + 1; k < i; k++) {
        ctick = ctick + weights[widx] * gts.ticks[k];
        cvalue = cvalue + weights[widx] * ((Number) valueAtIndex(gts, k)).doubleValue();
        widx++;
      }
      
      ctick = ctick / wsum;
      cvalue = cvalue / wsum;
      
      //
      // Compute weighted covariance and variance
      //
      
      double covar = 0.0D;
      double var = 0.0D;
      
      widx = 0;
      
      for (int k = j + 1; k < i; k++) {
        covar = covar + weights[widx] * (gts.ticks[k] - ctick) * (((Number) valueAtIndex(gts, k)).doubleValue() - cvalue);
        var = var + weights[widx] * (gts.ticks[k] - ctick) * (gts.ticks[k] - ctick);
        widx++;
      }
      
      covar = covar / wsum;
      var = var / wsum;
      
      //
      // Compute regression parameters
      //
      
      beta[1] = 0 == var ? 0.0D : covar / var;
      beta[0] = cvalue - ctick * beta[1];
      
    } else {
    
    //
    // Quadratic-or-more polynomial fit
    //
      
      // filling the container with the points
      
      List<WeightedObservedPoint> observations = new ArrayList<WeightedObservedPoint>();
      
      widx = 0;
      for (int k = j + 1; k < i; k++) {
        WeightedObservedPoint point = new WeightedObservedPoint(weights[widx], (double) gts.ticks[k], ((Number) valueAtIndex(gts, k)).doubleValue());
        observations.add(point);
        widx++;
      }
      
      PolynomialCurveFitter fitter = PolynomialCurveFitter.create(p);
      beta = fitter.fit(observations);
      observations.clear();
    
    }
    
    //
    // Compute value at 'tick'
    //
    
    double estimated = beta[0];
    double tmp = 1.0D;
    for (int u = 1; u < p + 1; u++){
      tmp *= tick;
      estimated += tmp * beta[u];  
    }
    
    return estimated;
  }
  
  public static double pointwise_lowess(GeoTimeSerie gts, int idx, long tick, int q, int p, double[] weights, double[] rho, double[] beta) throws WarpScriptException {
    return pointwise_lowess(gts, idx, tick, q, p, weights, rho, beta, false);
  }
  
  /**
   * Compute fast and robust version of LOWESS on a Geo Time Series,
   * with a polynomial fit of degree p > 0.
   * 
   * @see http://www.stat.washington.edu/courses/stat527/s14/readings/Cleveland_JASA_1979.pdf
   * 
   * @param gts Input GTS
   * @param q   Bandwith, i.e. number of nearest neighbours to consider when applying LOWESS
   * @param r   Robustness, i.e. number of robustifying iterations
   * @param d   Delta in µs, i.e. acceptable neighbourhood radius within which LOWESS is not recomputed
   *            close points are approximated by polynomial interpolation, d should remain < 0.1*(lasttick-firstick) in most cases
   * @param p   Degree, i.e. degree of the polynomial fit
   *            best usage p=1 or p=2 ; it will likely return an overflow error if p is too big
   * 
   * @param weights  : optional array that store the weights
   * @param rho      : optional array that store the robustness weights
   * @param inplace  : should the gts returned be the same object than the input
   * 
   * @return a smoothed GTS
   * @throws WarpScriptException 
   */
  public static GeoTimeSerie rlowess(GeoTimeSerie gts, int q, int r, long d, int p, double[] weights, double[] rho, boolean inplace) throws WarpScriptException {
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      throw new WarpScriptException("Can only smooth numeric Geo Time Series.");
    }
    
    if (q < 1) {
      throw new WarpScriptException("Bandwitdth parameter must be greater than 0");
    }
    
    if (r < 0) {
      throw new WarpScriptException("Robustness parameter must be greater or equal to 0");
    }
    
    if (d < 0) {
      throw new WarpScriptException("Delta parameter must be greater or equal to 0");
    }
    
    if (p < 1) {
      throw new WarpScriptException("Degree of polynomial fit must be greater than 0");
    }
    
    if (p > 9) {
      throw new WarpScriptException("Degree of polynomial fit should remain small (lower than 10)");
    }
    
    //
    // Sort the ticks
    //
    
    sort(gts, false);
    
    //
    // Check if number of missing values is reasonable
    // Note that (bucketcount - values) is not the number of missing values but a minorant
    //
    
    if (gts.bucketcount - gts.values > 500000) {
      throw new WarpScriptException("More than 500000 missing values");
    }    
        
    //
    // Check for duplicate ticks
    //
    
    long previous = -1;
    for (int t = 0; t < gts.values; t++) {
      long current = gts.ticks[t];
      if (previous == current){
        throw new WarpScriptException("Can't be applied on GTS with duplicate ticks");
      }
      previous = current;
    }
    
    //
    // At each step of robustifying operations,
    // we compute values in transient_smoothed using updated weights rho * weights
    //
    
    int size = isBucketized(gts) ? gts.bucketcount : gts.values;
    int sizehint = Math.max(gts.sizehint, (int) 1.1 * size);
    double[] transient_smoothed = new double[sizehint];
    
    int nvalues = q < size ? q : size;
    
    // Allocate an array for weights
    if (null == weights) {
      weights = new double[nvalues];
    } else {
      if (weights.length < nvalues) {
        throw new WarpScriptException("in rlowess weights array too small");
      }
    }
    
    // Allocate an array to update the weights through the robustness iterations and another for the absolute of the residuals
    // The weights used will be rho*weights
    double[] residual;
    if (r > 0){
      if (null == rho) {
        rho = new double[gts.values];
        Arrays.fill(rho, 1.0D);
      } else {
        if (rho.length < nvalues) {
          throw new WarpScriptException("in rlowess rho array too small");
        }
      }
      residual = new double[gts.values];
    } else {
      residual = null;
    }
    
    // Regression parameters
    double[] beta = new double[p + 1];
    
    //
    // Robustifying iterations
    //

    int r_iter = 0;
    while (r_iter < r + 1) {
      
      //
      // In order to speed up the computations,
      // we will skip some points pointed by iter.
      // We use iter_follower to interpolate the skipped points afterward.
      //
      
      Iterator<Long> iter = tickIterator(gts, false);
      Iterator<Long> iter_follower;
      if (0.0 == d){
        iter_follower = null;
      } else {
        iter_follower = tickIterator(gts,false);
      }
      
      // Index in the ticks/values array of the input gts
      int idx = 0;
      
      // Index in the ticks/values array of the output gts (values are also estimated for null points of the input)
      int ridx = 0;
      int ridx_last = 0;
      
      // Last tick estimated (set to -d-1 so (tick-last)>d at first iter) and its index in the result
      long last = d * (-1) - 1;
      int idx_last = 0;
      
      //
      // When we find a tick that is not within distance d of the last estimated tick,
      // then either we estimate it,
      // or if at least one tick has been skipped just before,
      // then we estimate the last skipped one and interpolate the others.
      // We then take back the loop from the former last skipped tick.
      //
      
      long last_skipped = 0;
      boolean skip = false;
      
      // Have skipped ticks been interpolated in last loop ?
      boolean resolved = false;
      
      // Current tick
      long tick = 0;
      
      while(iter.hasNext() || resolved) {
        
        if (!resolved) {
          tick = iter.next();
        } else {
          resolved = false;
        }
        
        // Skip points that are too close from the previous estimated one, unless its the last
        if (iter.hasNext() && (tick - last <= d)) {
          
          last_skipped = tick;
          skip = true;
          ridx++;
          
        } else {
          
          if (!skip) {
            
            // advance idx to the first neighbour at the right whose value is not null
            while(idx < gts.values - 1 && tick > tickAtIndex(gts, idx)) {
              idx++;
            }
            
            // compute value at tick
            transient_smoothed[ridx] = pointwise_lowess(gts, idx, tick, nvalues, p, weights, rho, beta);
            
            // update residual if tick had a non-null value
            if (r_iter < r && tick == tickAtIndex(gts, idx)) {
              residual[idx] = Math.abs(((Number) valueAtIndex(gts,idx)).doubleValue()-transient_smoothed[ridx]);
            }
            
            if (null != iter_follower) {
              iter_follower.next();
              last = tick;
              idx_last = idx;
              ridx_last = ridx;
            }
            ridx++;
            
          } else {
            
            if (!iter.hasNext() && (tick - last <= d)) {
              last_skipped = tick;
              ridx++;
            }
            
            // advance idx to the first neighbour at the right whose value is not null
            while(idx < gts.values - 1 && last_skipped > tickAtIndex(gts, idx)) {
              idx++;
            }
            
            // compute value at last_skipped tick
            transient_smoothed[ridx - 1] = pointwise_lowess(gts, idx, last_skipped, nvalues, p, weights, rho, beta);
            
            // update residual if tick had a non-null value
            if (r_iter < r && last_skipped == tickAtIndex(gts, idx)) {
              residual[idx] = Math.abs(((Number) valueAtIndex(gts,idx)).doubleValue()-transient_smoothed[ridx - 1]);
            }

            //
            // Linear interpolation of skipped points
            //
            
            double denom = last_skipped - last;
            long skipped = iter_follower.next();
            int ridx_s = ridx_last + 1;
            while (last_skipped > skipped) {
              
              // interpolate
              double alpha = (skipped - last) / denom;
              transient_smoothed[ridx_s] = alpha * transient_smoothed[ridx - 1] + (1 - alpha) * transient_smoothed[ridx_last];
              
              // update residual if tick had a non-null value
              int sidx;
              if (r_iter < r && 0 < (sidx = Arrays.binarySearch(gts.ticks, idx_last, idx, skipped))) {
                residual[sidx] = Math.abs(((Number) valueAtIndex(gts,sidx)).doubleValue()-transient_smoothed[ridx_s]);
              }
              
              skipped = iter_follower.next();
              ridx_s++;
              
            }
            
            if (iter.hasNext() || (tick - last > d)) {
              //updates
              skip = false;
              resolved = true;
              last = last_skipped;
              idx_last = idx;
              ridx_last = ridx - 1;
            }
            
          }
        }
      }

      //
      // Update robustifying weights (except last time or if r is 0)
      //

      if (r_iter < r) {
        
        // rho's values will be recomputed anyway so sorted can borrow its body
        double[] sorted = rho;
        sorted = Arrays.copyOf(residual, gts.values);
        Arrays.sort(sorted);
        
        // compute median of abs(residual)
        double median;
        if (gts.values % 2 == 0) {
          median = (sorted[gts.values/2] + sorted[gts.values/2 - 1])/2;
        } else {
          median = sorted[gts.values/2];
        }
        
        // compute h = 6 * median and rho = bisquare(|residual|/h)
        double h = 6 * median;
        
        for (int k = 0; k < gts.values; k++) {
          if (0 == h){
            rho[k] = 1.0D;
          } else{
            double u = residual[k] / h;
            
            if (u >= 1.0) {
              rho[k] = 0.0D;
            } else {
              rho[k] = 1.0D - u * u;
              rho[k] = rho[k] * rho[k];
            }
          }
        }
      }
            
      r_iter++;
    }
    
    //
    // Copying result to output
    //
    
    boolean hasLocations = null != gts.locations;
    boolean hasElevations = null != gts.elevations;
    
    //
    // We separate case without or with missing values.
    //
    
    if (!isBucketized(gts) || (gts.values == gts.bucketcount && gts.lastbucket == gts.ticks[gts.values - 1] && gts.lastbucket - gts.bucketspan * (gts.bucketcount - 1) == gts.ticks[0])) {
      if (inplace) {

        if (TYPE.LONG == gts.type) {
          gts.longValues = null;
          gts.type = TYPE.DOUBLE;
        }

        gts.doubleValues = transient_smoothed;
        return gts;
        
      } else {
        
        GeoTimeSerie smoothed = gts.cloneEmpty(sizehint);
        try {
          smoothed.reset(Arrays.copyOf(gts.ticks,sizehint), hasLocations ? Arrays.copyOf(gts.locations,sizehint) : null, hasElevations ? Arrays.copyOf(gts.elevations,sizehint) : null, transient_smoothed, size);
        } catch (IOException ioe) {
          throw new WarpScriptException("IOException in reset method: " + ioe.getMessage());
        }
        
        return smoothed;
        
      }
    } else {
      
      //
      // Case with missing values
      //
      
      if (inplace) {
        
        // TODO(JCV): need to unit test location and elevation settings in this case
        
        if (hasLocations && gts.locations.length != sizehint) {
          gts.locations = Arrays.copyOf(gts.locations, sizehint);
        }
        
        if (hasElevations && gts.elevations.length != sizehint) {
          gts.elevations = Arrays.copyOf(gts.elevations, sizehint);
        }
        
        if (gts.ticks.length != sizehint) {
          gts.ticks = Arrays.copyOf(gts.ticks, sizehint);
        }
        
        // We try to allocate the lesser additionnal memory so we
        // fill locations and elevations backward with values upfront in the same array
        
        if (hasLocations || hasElevations) {
          
          Iterator<Long> iter = tickIterator(gts, true);
          int idx = gts.values - 1;
          
          int idx_new = gts.bucketcount;
          while (iter.hasNext()) {
            
            long tick = iter.next();
            idx_new--;
            
            // search if tick was present in original gts
            while (idx > 0 && tick < gts.ticks[idx]) {
              idx--;
            }
          
            if (hasLocations) {
              gts.locations[idx_new] = tick == gts.ticks[idx] ? gts.locations[idx] : GeoTimeSerie.NO_LOCATION;
            }
            
            if (hasElevations) {
              gts.elevations[idx_new] = tick == gts.ticks[idx] ? gts.locations[idx] : GeoTimeSerie.NO_ELEVATION;
            }
            
            gts.ticks[idx_new] = tick;
            
          }
        }

        if (TYPE.LONG == gts.type) {
          gts.longValues = null;
          gts.type = TYPE.DOUBLE;
        }
        gts.doubleValues = transient_smoothed;
        gts.values = size;
        return gts;
        
      } else {
        
        GeoTimeSerie smoothed = gts.cloneEmpty(sizehint);
        
        // ticks
        long[] ticks = new long[sizehint];
        int idx = 0;
        Iterator<Long> iter = tickIterator(gts, false);
        while (iter.hasNext()){
          ticks[idx] = iter.next();
          idx++;
        }
        
        // locations
        int v = 0;
        long[] locations = null;
        if (hasLocations) {
          locations = new long[sizehint];
          for (int u = 0; u < size; u++) {
            v = Arrays.binarySearch(gts.ticks, v, gts.values, smoothed.ticks[u]);
            locations[u] = v < 0 ? GeoTimeSerie.NO_LOCATION : gts.locations[v];
          }
        }
        
        // elevations
        v = 0;
        long[] elevations = null;
        if (hasElevations) {
          elevations = new long[sizehint];
          for (int u = 0; u < size; u++) {
            v = Arrays.binarySearch(gts.ticks, v, gts.values, smoothed.ticks[u]);
            elevations[u] = v < 0 ? GeoTimeSerie.NO_ELEVATION : gts.elevations[v];
          }
        }
        
        try {
          smoothed.reset(ticks, locations, elevations, transient_smoothed, size);
        } catch (IOException ioe) {
          throw new WarpScriptException("IOException in reset method: " + ioe.getMessage());
        }
        
        return smoothed;
        
      }
    }
  }

  public static GeoTimeSerie rlowess(GeoTimeSerie gts, int q, int r, long d, int p) throws WarpScriptException {
    return rlowess(gts, q, r, d, p, null, null, false);
  }
  
  /**
   * Version of LOWESS used in stl
   * 
   * @param fromGTS    : GTS from which data used for estimation is taken
   * @param toGTS      : GTS where results are saved 
   * @param neighbours : number of nearest neighbours to take into account when computing lowess (bandwitdh)
   * @param degree     : degree of polynomial fit in lowess
   * @param jump       : number of bucket to skip when computing lowess (to speed it up)
   * @param weights    : optional array that store the weights
   * @param rho        : optional array that store the robustness weights
   */
  public static void lowess_stl(GeoTimeSerie fromGTS, GeoTimeSerie toGTS, int neighbours, int degree, int jump, double[] weights, double[] rho) throws WarpScriptException {
    if (!isBucketized(fromGTS)) {
      throw new WarpScriptException("lowess_stl method works with bucketized gts only");
    }
    
    if (fromGTS == toGTS) {
      throw new WarpScriptException("in lowess_stl method, fromGTS and toGTS can't be the same object. Please consider using rlowess method instead");
    }
    
    sort(fromGTS);
    
    // estimate all points but skip jump_s points between each
    // (we are starting at lastbucket and are going backward)
    int idx = fromGTS.values - 1;
    
    // we want to end on the oldest bucket
    int rest = (fromGTS.bucketcount - 1) % (jump + 1);
    for (int j = 0; j <= (fromGTS.bucketcount - 1) / (jump + 1); j++) {
      
      // calculate tick
      long tick = fromGTS.lastbucket - (j * (jump + 1) + rest) * fromGTS.bucketspan;
      
      // take back idx to the first neighbour at the left whose value is not null
      while(idx > -1 && tick < tickAtIndex(fromGTS, idx)) {
        idx--;
      }
      
      // estimate value
      double estimated = pointwise_lowess(fromGTS, idx, tick, neighbours, degree, weights, rho, null, true);
      setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, estimated, true);
      
    }    
    
    // interpolate skipped points
    for (int j = 0; j < (fromGTS.bucketcount - 1) / (jump + 1); j++) {
      
      int right = j * (jump + 1) + rest;
      int left = (j + 1) * (jump + 1) + rest;
      double denom = left - right;
      long righttick = fromGTS.lastbucket - right * fromGTS.bucketspan;
      long lefttick = fromGTS.lastbucket - left * fromGTS.bucketspan;
      
      for (int r = 1; r < jump + 1; r++) {
        
        int middle = r + j * (jump + 1) + rest;
        long tick = fromGTS.lastbucket - middle * fromGTS.bucketspan;
        
        double alpha = (middle - right) / denom;
        double interpolated = alpha * ((Number) valueAtTick(toGTS, lefttick)).doubleValue() + (1 - alpha) * ((Number) valueAtTick(toGTS, righttick)).doubleValue();
        setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, interpolated, true);
        
      }
    }
    
    // estimate the most recent point in case it has been jumped
    if (0 != rest) {
      
      // take back idx to the first neighbour at the left whose value is not null if any
      idx = fromGTS.values - 1;
      while(idx > -1 && fromGTS.lastbucket < tickAtIndex(fromGTS, idx)) {
        idx--;
      }
      
      // estimate value
      double estimated = pointwise_lowess(fromGTS, idx, fromGTS.lastbucket, neighbours, degree, weights, rho, null, true);          
      setValue(toGTS, fromGTS.lastbucket, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, estimated, true);
      
      // interpolate skipped points
      int right = 0;
      int left = rest;
      double denom = left - right;
      long lefttick = fromGTS.lastbucket - left * fromGTS.bucketspan;
      
      for (int r = 1; r < rest; r++) {
        long tick = fromGTS.lastbucket - r * fromGTS.bucketspan;
        
        double alpha = (r - right) / denom;
        double interpolated = alpha * ((Number) valueAtTick(toGTS, lefttick)).doubleValue() + (1 - alpha) * estimated;
        setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, interpolated, true);
      }      
    }
  }
  
  /**
   * Compute STL i.e. Seasonal-Trend decompostition precedure based on LOWESS
   * @see http://www.wessa.net/download/stl.pdf
   * 
   * Global parameters:
   * @param gts                : Input GTS, must be bucketized
   * @param buckets_per_period : number of buckets for one period of the seasonality
   * @param inner Precision    : number of inner loops (to make the decomposition)
   * @param outer Robustness   : number of outer loops (to alleviate the impact of outliers upon the decompsition)
   * @param ...
   * 
   * Optional sets of parameters shared by call of lowess of the same kind:
   * @param neighbour_*        : Bandwith, i.e. number of nearest neighbours to consider when applying LOWESS
   * @param degree_*           : Degree, i.e. degree of the polynomial fit
   * @param jump_*             : Jump, i.e. number of bucket to skip to speed up comptutation. These buckets are interpolated afterward.
   * 
   * With:
   * @param _* = _s            : lowess calls during seasonal extract step
   * @param _* = _l            : lowess calls during low frequency filtering step
   * @param _* = _t            : lowess calls during trend extract step
   * @param _* = _p            : lowess calls during post seasonal smoothing step
   * 
   * @return a list of 2 GTS consisting of the seasonal and trend part of the decomposition.
   */
  public static List<GeoTimeSerie> stl(
      GeoTimeSerie gts,
      
      // big picture STL parameters
      int buckets_per_period,
      int inner,
      int outer,
      
      // lowess parameters for seasonal extract
      int neighbour_s,
      int degree_s,
      int jump_s,
            
      // lowess parameters for low-pass filtering
      int neighbour_l,
      int degree_l,
      int jump_l,
      
      // lowess parameters for trend extract
      int neighbour_t,
      int degree_t,
      int jump_t,
      
      // lowess parameters for seasonal post-smoothing
      int neighbour_p,
      int degree_p,
      int jump_p
            
      ) throws WarpScriptException {
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      throw new WarpScriptException("Can only be applied on numeric Geo Time Series.");
    }
    
    if (!isBucketized(gts)) {
      throw new WarpScriptException("Can only be applied on bucketized Geo Time Series");
    }
    
    //
    // Sort the GTS
    //
    
    sort(gts, false);
    
    //
    // Allocate tables: Y = S + T + R
    // Y: initial GTS
    // S: seasonal
    // T: trend
    // R: residual
    //
    
    int nonnull = gts.values;
    int size = gts.bucketcount;
    
    // limit size at the moment
    if (size - nonnull > 500000) {
      throw new WarpScriptException("More than 500000 missing values");
    }
        
    // At each iteration of inner loop, seasonal will be augmented of 2*bpp buckets.
    // The current implementation fill every gap in seasonal with a value estimated by lowess.
    // Hence sizehint choice.
    // TODO(JCV): maybe rethink STL so that it can handle missing values better.
    int sizehint = size + 2 * buckets_per_period;
    GeoTimeSerie seasonal = gts.cloneEmpty(sizehint);
    try {
      seasonal.reset(Arrays.copyOf(gts.ticks,sizehint), null, null, new double[sizehint], nonnull);
    } catch (IOException ioe) {
      throw new WarpScriptException("IOException in reset method: " + ioe.getMessage());
    }
    
    
    GeoTimeSerie trend = gts.cloneEmpty(sizehint);
    try {
      trend.reset(Arrays.copyOf(gts.ticks,sizehint), null, null, new double[sizehint], nonnull);
    } catch (IOException ioe) {
      throw new WarpScriptException("IOException in reset method: " + ioe.getMessage());
    }
    
    // lowpassed will borrow the body of trend in step 3
    GeoTimeSerie lowpassed = trend;
    
    // Allocate an array to update the weights through the outer loop
    // The weights used will be rho*weights
    double[] rho = new double[nonnull];
    Arrays.fill(rho, 1.0D);
    
    // residual will borrow body of rho
    double[] residual = rho;
    
    // Weights used in lowess computations
    int nei = Math.max(Math.max(neighbour_s, neighbour_l), neighbour_t);
    double[] weights = new double[nei];
    // FIXME(JCV): maybe smarter size to put in here ?
    
    //
    // Outer loop
    //
    
    for (int s = 0; s < outer + 1; s++) {
      
      //
      // Inner loop
      //
      
      for (int k = 0 ; k < inner; k++) {
        
        //
        // Step 1: Detrending
        //
        
        int idx_t = 0;
        for (int idx = 0 ; idx < nonnull; idx++){
          idx_t = Arrays.binarySearch(trend.ticks, idx_t, nonnull, gts.ticks[idx]);
          seasonal.doubleValues[idx] = ((Number) valueAtIndex(gts,idx)).doubleValue() - trend.doubleValues[idx_t];
        }
        
        //
        // Step 2: Cycle-subseries smoothing
        //
        
        GeoTimeSerie subCycle = null;
        GeoTimeSerie subRho = null;
        
        // smoothing of each cycle-subserie
        for (int c = 0; c < buckets_per_period; c++ ) {
          
          // extracting a cycle-subserie and extending by one value at both ends
          subCycle = subCycleSerie(seasonal, seasonal.lastbucket - c * seasonal.bucketspan, buckets_per_period, true, subCycle);
          subCycle.lastbucket += subCycle.bucketspan;
          subCycle.bucketcount += 2;
          
          // extracting subRho
          if (s > 0) {
            double[] tmp = seasonal.doubleValues;
            seasonal.doubleValues = rho;
            subRho = subCycleSerie(seasonal, seasonal.lastbucket - c * seasonal.bucketspan, buckets_per_period, true, subRho);
            seasonal.doubleValues = tmp;
          }
          
          // applying lowess
          lowess_stl(subCycle, seasonal, neighbour_s, degree_s, jump_s, weights, s > 0 ? subRho.doubleValues : rho);
        }
        
        /*
         * Concretely, with the added points, seasonal.lastbucket is 1 period behind the reality,
         * and it has twice bpp more buckets than its bucketcount.
         */
        
        // Updating bucket parameters of seasonal for clarity
        seasonal.lastbucket += seasonal.bucketspan * buckets_per_period;
        seasonal.bucketcount += 2 * buckets_per_period;        
        
        //
        // Step 3: Low-Pass Filtering of Smoothed Cycle-subseries
        //

        sort(seasonal);
        
        // FIXME(JCV): what happens if buckets_per_period < bucketcount / buckets_per_period ?
        
        // Applying first moving average of size bpp
        
        // First average
        double sum = 0;
        for (int r = 0; r < buckets_per_period; r++) {
          sum += seasonal.doubleValues[r];
        }
        lowpassed.doubleValues[0] = sum / buckets_per_period;
        
        // other averages
        for (int r = 1; r < seasonal.bucketcount - buckets_per_period + 1; r++) {
          sum += seasonal.doubleValues[r + buckets_per_period - 1] - seasonal.doubleValues[r - 1];
          lowpassed.doubleValues[r] = sum / buckets_per_period;
        }
        
        // Applying 2nd moving average of size bpp
        sum = 0;
        for (int r = 0; r < buckets_per_period; r++) {
          sum += lowpassed.doubleValues[r];
        }
        double tmp = lowpassed.doubleValues[0];
        lowpassed.doubleValues[0] = sum / buckets_per_period;
        for (int r = 1; r <= seasonal.bucketcount - 2 * buckets_per_period + 1; r++) {
          sum += lowpassed.doubleValues[r + buckets_per_period - 1] - tmp;
          tmp = lowpassed.doubleValues[r];
          lowpassed.doubleValues[r] = sum / buckets_per_period;
        }
        
        // Applying 3rd moving average of size 3
        for (int r = 0; r < seasonal.bucketcount - 2 * buckets_per_period; r++) {
          lowpassed.doubleValues[r] += lowpassed.doubleValues[r + 1] + lowpassed.doubleValues[r + 2];
          lowpassed.doubleValues[r] /= 3;
        }
        
        // Update size of gts
        lowpassed.bucketcount = seasonal.bucketcount - 2 * buckets_per_period;
        lowpassed.lastbucket = seasonal.lastbucket - buckets_per_period * seasonal.bucketspan;
        lowpassed.values = lowpassed.bucketcount;

        // Lowess_l
        lowpassed = rlowess(lowpassed, neighbour_l, 0, (jump_l + 1) * lowpassed.bucketspan, degree_l, weights, null, true);

        //
        // Step 4: Detrending of Smoothed Cycle-Subseries
        //
        
        // shifting seasonal bucket parameters back
        seasonal.lastbucket -= seasonal.bucketspan * buckets_per_period;
        seasonal.bucketcount -= 2 * buckets_per_period;        
        
        if (seasonal.bucketcount != lowpassed.values) {
          throw new WarpScriptException("stl impl error #1: " + seasonal.values + " vs " + lowpassed.values);
        }
        
        for (int r = 0; r < seasonal.bucketcount; r++) {
          seasonal.doubleValues[r] = seasonal.doubleValues[r + buckets_per_period] - lowpassed.doubleValues[r];
          seasonal.ticks[r] = seasonal.ticks[r + buckets_per_period];
        }
        
        // trim seasonal back
        seasonal.values = seasonal.bucketcount;

        //
        // Step 5: Deseasonalizing
        //
        
        int idx_s = 0;
        for (int idx = 0 ; idx < nonnull; idx++){
          idx_s = Arrays.binarySearch(seasonal.ticks, idx_s, nonnull, gts.ticks[idx]);
          trend.doubleValues[idx] = ((Number) valueAtIndex(gts,idx)).doubleValue() - seasonal.doubleValues[idx_s];
        }
        
        trend.values = nonnull;
        trend.lastbucket = gts.lastbucket;
        trend.bucketspan = gts.bucketspan;
        trend.bucketcount = size;

        //
        // Step 6: Trend smoothing
        //
        
        // FIXME(JCV): currently all buckets are estimated or interpolated, but it is not necessary to estimate ones with no value unless it is the last iteration
        // But won't gain that much in speed if enough points are already interpolated.
        
        trend = rlowess(trend, neighbour_t, 0, (jump_t + 1) * trend.bucketspan, degree_t, weights, rho, true);
      }
      
      //
      // Robustifying operations of outer loop (except last time)
      //

      if (s < outer) {
        
        // compute residual
        int idx_s = 0;
        int idx_t = 0;
        for (int idx = 0 ; idx < nonnull; idx++){
          idx_s = Arrays.binarySearch(seasonal.ticks, idx_s, nonnull, gts.ticks[idx]);
          //idx_t = Arrays.binarySearch(trend.ticks, idx_t, nonnull, gts.ticks[idx]);
          // we assume idx_t == idx_s
          idx_t = idx_s;
          residual[idx] = Math.abs(((Number) valueAtIndex(gts,idx)).doubleValue() - seasonal.doubleValues[idx_s] - trend.doubleValues[idx_t]);
        }
        
        //
        // Update robustifying weights 
        //
                
        // compute median of abs(residual)
        double median;
        double[] sorted = Arrays.copyOf(residual, gts.values);
        Arrays.sort(sorted);
        if (gts.values % 2 == 0) {
          median = (sorted[gts.values/2] + sorted[gts.values/2 - 1])/2;
        } else {
          median = sorted[gts.values/2];
        }
        
        // compute h = 6 * median and rho = bisquare(|residual|/h)
        double h = 6 * median;
        
        for (int k = 0; k < gts.values; k++) {
          if (0 == h){
            rho[k] = 1.0D;
          } else{
            double u = residual[k] / h;
            
            if (u >= 1.0) {
              rho[k] = 0.0D;
            } else {
              rho[k] = 1.0D - u * u;
              rho[k] = rho[k] * rho[k];
            }
          }
        }
      }
    }
    
    //
    // Post seasonal smoothing
    //
    
    if (neighbour_p > 0) {
      seasonal = rlowess(seasonal, neighbour_p, 0, (jump_p + 1) * seasonal.bucketspan, degree_p);
    }
    
    //
    // Locations and elevations
    //
    
    int v = 0;
    if (null != gts.locations) {
      for (int u = 0; u < size; u++) {
        v = Arrays.binarySearch(gts.ticks, v, nonnull, seasonal.ticks[u]);
        seasonal.locations[u] = v < 0 ? GeoTimeSerie.NO_LOCATION : gts.locations[v];
        trend.locations[u] = seasonal.locations[u];
      }
    } else {
      seasonal.locations = null;
      trend.locations = null;
    }
    
    v = 0;
    if (null != gts.elevations) {
      for (int u = 0; u < size; u++) {
        v = Arrays.binarySearch(gts.ticks, v, nonnull, seasonal.ticks[u]);
        seasonal.elevations[u] = v < 0 ? GeoTimeSerie.NO_ELEVATION : gts.elevations[v];
        trend.elevations[u] = seasonal.elevations[u];
      }
    } else {
      seasonal.elevations = null;
      trend.elevations = null;
    }    
    
    //
    // Output
    //
    
    String prefix = (null == gts.getName()) || (0 == gts.getName().length()) ? "" : gts.getName() + "_";
    seasonal.setName(prefix + "seasonal");
    trend.setName(prefix + "trend");
    
    List<GeoTimeSerie> output = new ArrayList<GeoTimeSerie>();
    output.add(seasonal);
    output.add(trend);
    
    return output;
  }
  
  /**
   * Copy geo infos (location + elevation) from GTS 'from' into GTS 'to'
   * Only infos for matching ticks with values in 'to' will be copied
   * 
   * @param from
   * @param to
   */
  public static void copyGeo(GeoTimeSerie from, GeoTimeSerie to) {
    // Sort both GTS
    GTSHelper.sort(from, false);
    GTSHelper.sort(to, false);
    
    int fromidx = 0;
    int toidx = 0;
    
    while(toidx < to.values && fromidx < from.values) {
      long fromtick = GTSHelper.tickAtIndex(from, fromidx);
      long totick = GTSHelper.tickAtIndex(to, toidx);
      
      // Advance fromidx until fromtick >= totick
      while(fromidx < from.values && fromtick < totick) {
        fromidx++;
        fromtick = GTSHelper.tickAtIndex(from, fromidx);
      }
      
      if (fromidx >= from.values) {
        break;
      }
      
      // Advance toidx until totick == fromtick
      while(toidx < to.values && totick < fromtick) {
        toidx++;
        totick = GTSHelper.tickAtIndex(to, toidx);
      }
      
      if (toidx >= to.values) {
        break;
      }
      
      if (totick == fromtick) {
        long location = GTSHelper.locationAtIndex(from, fromidx);
        long elevation = GTSHelper.elevationAtIndex(from, fromidx);
        
        // Set location + elevation at 'toidx'
        GTSHelper.setLocationAtIndex(to, toidx, location);
        GTSHelper.setElevationAtIndex(to, toidx, elevation);
        fromidx++;
      }
    }
  }

  public static int[] getFirstLastTicks(long[] ticks) {
    //
    // Determine first and last ticks
    //
    
    long firsttick = Long.MAX_VALUE;
    long lasttick = Long.MIN_VALUE;
    
    int firstidx = -1;
    int lastidx = -1;
    
    for (int i = 0; i < ticks.length; i++) {
      if (ticks[i] < firsttick) {
        firstidx = i;
        firsttick = ticks[i];
      }
      if (ticks[i] > lasttick) {
        lastidx = i;
        lasttick = ticks[i];
      }
    }
  
    return new int[] { firstidx, lastidx };
  }
  
  public static boolean geowithin(GeoXPShape shape, GeoTimeSerie gts) {
    boolean hasLocations = false;
    for (int i = 0; i < gts.values; i++) {      
      long location = GTSHelper.locationAtIndex(gts, i);
      if (GeoTimeSerie.NO_LOCATION != location) {
        hasLocations = true;
        if (!GeoXPLib.isGeoXPPointInGeoXPShape(location, shape)) {
          return false;
        }
      }
    }
    return hasLocations;
  }

  public static boolean geointersects(GeoXPShape shape, GeoTimeSerie gts) {
    for (int i = 0; i < gts.values; i++) {      
      long location = GTSHelper.locationAtIndex(gts, i);
      if (GeoTimeSerie.NO_LOCATION != location) {
        if (GeoXPLib.isGeoXPPointInGeoXPShape(location, shape)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Return shrinked versions of the input GTS containing only the ticks which have
   * values in all input GTS.
   * 
   * @param series
   * @return
   */  
  public static List<GeoTimeSerie> commonTicks(List<GeoTimeSerie> series) {
    
    if (1 == series.size()) {
      GeoTimeSerie serie = series.get(0).clone();
      List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
      result.add(serie);
      return result;
    }
    
    //
    // Start by sorting the GTS in chronological order
    //
    
    for (GeoTimeSerie serie: series) {
      GTSHelper.sort(serie);
    }
    
    //
    // Now run a sweeping line algorithm
    //
    
    int[] idx = new int[series.size()];
    
    //
    // Determine which GTS has the least number of ticks, it will be the leader
    //
    
    int minticks = Integer.MAX_VALUE;
    int leader = -1;
    
    for (int i = 0; i < series.size(); i++) {
      if (series.get(i).values < minticks) {
        leader = i;
        minticks = series.get(i).values;
      }
    }
    
    GeoTimeSerie leadergts = series.get(leader);
    
    // Build resulting GTS
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    for (int i = 0; i < series.size(); i++) {
      result.add(series.get(i).cloneEmpty(series.get(i).values / 2));
    }
    
    //
    // Return immediately if minticks is 0, meaning that there is one empty GTS
    //
    
    if (0 == minticks) {
      return result;
    }
    
    while(true) {
      //
      // Advance all indices until the timestamp is >= that of the leader
      //
      
      // Set the target tick to the current one of the leader
      long target = leadergts.ticks[idx[leader]];

      boolean match = true;
      
      for (int i = 0; i < series.size(); i++) {
        if (i == leader) {
          continue;
        }

        GeoTimeSerie serie = series.get(i);
        
        //
        // Advance index until we reach or pass target
        //
        
        while(idx[i] < serie.values && serie.ticks[idx[i]] < target) {
          idx[i]++;
        }
        
        //
        // We've reached the end of one of the GTS, we know we're done
        //
        
        if (idx[i] >= serie.values) {
          return result;
        }
        
        //
        // We've reached a tick which is greater than the leader's current tick, we need to move the leader now
        //
        
        if (serie.ticks[idx[i]] > target) {
          //
          // Advance the leader
          //
          
          while(idx[leader] < leadergts.values && leadergts.ticks[idx[leader]] < serie.ticks[idx[i]]) {
            idx[leader]++;
          }
          
          if (idx[leader] >= leadergts.values) {
            return result;
          }
          match = false;
          break; 
        }
      }
      
      //
      // We have a march of all the ticks, add the current datapoint to the resulting GTS
      //
      
      if (match) {
        for (int i = 0; i < series.size(); i++) {
          GeoTimeSerie serie = series.get(i);
          GTSHelper.setValue(result.get(i),
            GTSHelper.tickAtIndex(serie, idx[i]),
            GTSHelper.locationAtIndex(serie, idx[i]),
            GTSHelper.elevationAtIndex(serie, idx[i]),
            GTSHelper.valueAtIndex(serie, idx[i]),
            false);
        }   
        idx[leader]++;
        if (idx[leader] >= leadergts.values) {
          return result;
        }
      }
    }
  }
  
  public static List<Number> bbox(GeoTimeSerie gts) {
    double maxlat = -90.0D;
    double minlat = 90.0D;
    double minlon = 180.0D;
    double maxlon = -180.0D;

    List<Number> bbox = new ArrayList<Number>();
    
    if (null == gts.locations) {
      bbox.add(-90.0D);
      bbox.add(-180.0D);
      bbox.add(90.0D);
      bbox.add(180.0D);
      return bbox;
    }
    
    for (int i = 0; i < gts.values; i++) {
      if (GeoTimeSerie.NO_LOCATION == gts.locations[i]) {
        continue;
      }
      
      double[] latlon = GeoXPLib.fromGeoXPPoint(gts.locations[i]);
      
      if (latlon[0] < minlat) {
        minlat = latlon[0];
      }
      if (latlon[0] > maxlat) {
        maxlat = latlon[0];
      }
      if (latlon[1] < minlon) {
        minlon = latlon[1];
      }
      if (latlon[1] > maxlon) {
        maxlon = latlon[1];
      }
    }
    
    bbox.add(minlat);
    bbox.add(minlon);
    bbox.add(maxlat);
    bbox.add(maxlon);

    return bbox;
  }
 
  /**
   * Compute conditional probabilities given a GTS considering the values as the concatenation
   * of given events plus the event for which we want to compute the probability, separated by 'separator'
   * 
   * If 'sepearator' is null then we simply compute the probability of values instead of conditional probabilities
   * 
   * @param gts
   * @param separator
   * @return
   */
  public static GeoTimeSerie cprob(GeoTimeSerie gts, String separator) throws WarpScriptException {
    
    Map<Object,AtomicInteger> histogram = new HashMap<Object, AtomicInteger>();

    GeoTimeSerie prob = gts.cloneEmpty();

    if (null == separator) {      
      long total = 0L;
      
      for (int i = 0; i < gts.values; i++) {
        Object value = GTSHelper.valueAtIndex(gts, i);
        AtomicInteger count = histogram.get(value);
        if (null == count) {
          count = new AtomicInteger(0);
          histogram.put(value, count);
        }
        count.addAndGet(1);
        total++;
      }
      
      for (int i = 0; i < gts.values; i++) {
        long timestamp = GTSHelper.tickAtIndex(gts, i);
        long geoxppoint = GTSHelper.locationAtIndex(gts, i);
        long elevation = GTSHelper.elevationAtIndex(gts, i);
        Object value = GTSHelper.valueAtIndex(gts, i);
        double p = histogram.get(value).doubleValue() / total;
        GTSHelper.setValue(prob, timestamp, geoxppoint, elevation, p, false);
      }
      
      return prob;
    }
    
    //
    // Sort 'gts' by value so we group the 'given' events by common set of values
    //
    
    GTSHelper.valueSort(gts);
    
    int idx = 0;
    
    while(idx < gts.values) {
      //
      // Extract 'given' events
      //
      
      Object val = GTSHelper.valueAtIndex(gts, idx);
      
      if (!(val instanceof String)) {
        throw new WarpScriptException("Can only compute conditional probabilities for String Geo Time Series.");
      }
      
      int lastsep = val.toString().lastIndexOf(separator);
      
      if (-1 == lastsep) {
        throw new WarpScriptException("Separator not found, unable to isolate given events.");
      }
      
      String given = val.toString().substring(0, lastsep);
     
      histogram.clear();
      long total = 0;
      
      int subidx = idx;
      
      while(subidx < gts.values) {
        
        val = GTSHelper.valueAtIndex(gts, subidx);
        
        lastsep = val.toString().lastIndexOf(separator);
        
        if (-1 == lastsep) {
          throw new WarpScriptException("Separator not found, unable to isolate given events.");
        }
        
        String givenEvents = val.toString().substring(0, lastsep);
        
        if (!givenEvents.equals(given)) {
          break;
        }
        
        String event = val.toString().substring(lastsep + separator.length()).trim();
        
        AtomicInteger count = histogram.get(event);
        
        if (null == count) {
          count = new AtomicInteger(0);
          histogram.put(event, count);
        }
        
        count.addAndGet(1);
        total++;
        
        subidx++;
      }
      
      for (int i = idx; i < subidx; i++) {
        val = GTSHelper.valueAtIndex(gts, i);
        
        lastsep = val.toString().lastIndexOf(separator);
        
        String event = val.toString().substring(lastsep + separator.length());

        long timestamp = GTSHelper.tickAtIndex(gts, i);
        long location = GTSHelper.locationAtIndex(gts, i);
        long elevation = GTSHelper.elevationAtIndex(gts, i);
        
        double p = histogram.get(event).doubleValue() / total;
        
        GTSHelper.setValue(prob, timestamp, location, elevation, p, false);
      }
      
      idx = subidx;
    } 
    
    return prob;
  }
  
  /**
   * Convert a GTS into a GTS of the probability associated with the value present at each tick
   * given the value histogram
   * 
   * @param gts
   * @return
   */
  public static GeoTimeSerie prob(GeoTimeSerie gts) {
    Map<Object, Long> histogram = valueHistogram(gts);
    
    GeoTimeSerie prob = gts.cloneEmpty(gts.values);
    
    for (int i = 0; i < gts.values; i++) {
      long timestamp = tickAtIndex(gts, i);
      long geoxppoint = locationAtIndex(gts, i);
      long elevation = elevationAtIndex(gts, i);
      Object value = valueAtIndex(gts, i);
      
      setValue(prob, timestamp, geoxppoint, elevation, histogram.get(value).doubleValue() / gts.values, false);
    }
    
    return prob;
  }
  
  public static GeoTimeSerie lttb(GeoTimeSerie gts, int threshold, boolean timebased) throws WarpScriptException {
    //
    // If the GTS has less than threshold values, return it as is
    //
    
    if (gts.values <= threshold - 2) {
      return gts;
    }
    
    // Exclude the left and right datapoints, they will be retained.
    threshold = threshold - 2;
    
    if (threshold <= 0) {
      throw new WarpScriptException("Threshold MUST be >= 3.");
    }
    
    int bucketsize = (int) Math.ceil((double) gts.values / (double) threshold);
    
    // Sort GTS
    GTSHelper.sort(gts);

    long timebucket = (long) Math.ceil((gts.ticks[gts.values - 1] - gts.ticks[0]) / (double) threshold); 
    long firsttick = gts.ticks[0];
    
    GeoTimeSerie sampled = gts.cloneEmpty(threshold  + 2);

    // Add first datapoint
    GTSHelper.setValue(sampled, GTSHelper.tickAtIndex(gts,0), GTSHelper.locationAtIndex(gts, 0), GTSHelper.elevationAtIndex(gts, 0), GTSHelper.valueAtIndex(gts, 0), false);

    int refidx = 0;
    
    int firstinrange = 1;
    int lastinrange = -1;
    
    for (int i = 0; i < threshold; i++) {
      
      if (lastinrange >= gts.values - 1) {
        break;
      }
      
      //
      // Determine the ticks to consider when computing the next datapoint
      //
      
      if (timebased) {
        long lowerts = firsttick + i * timebucket;
        long upperts = lowerts + timebucket - 1;
        lastinrange++;
        boolean empty = true;
        for (int j = lastinrange; j < gts.values - 1; j++) {
          if (firstinrange < lastinrange && gts.ticks[j] >= lowerts) {
            firstinrange = j;
          }
          if (gts.ticks[j] <= upperts) {
            lastinrange = j;
            empty = false;
          } else {
            break;
          }
        }
        if (empty) {
          lastinrange--;
          continue;
        }
        if (firstinrange > lastinrange) {
          continue;
        }
      } else {
        firstinrange = ++lastinrange;
        lastinrange = firstinrange + bucketsize - 1;
        if (lastinrange >= gts.values) {
          lastinrange = gts.values - 2;
        }
        if (firstinrange > lastinrange) {
          continue;
        }
      }
      
      //
      // Compute the average value on the range and the average tick
      //
      
      double ticksum = 0.0D;
      double valuesum = 0.0D;
      
      for (int j = firstinrange; j <= lastinrange; j++) {
        ticksum += gts.ticks[j];
        valuesum += ((Number) GTSHelper.valueAtIndex(gts, j)).doubleValue();
      }
      
      double tickavg = ticksum / (lastinrange - firstinrange + 1);
      double valueavg = valuesum / (lastinrange - firstinrange + 1);
      
      //
      // Now compute the triangle area and retain the point in the range which maximizes it
      //
      
      double maxarea = -1.0D;
      
      double refvalue = ((Number) GTSHelper.valueAtIndex(gts, refidx)).doubleValue();
      double reftick = gts.ticks[refidx];
      
      int nextref = -1;
      
      for (int j = firstinrange; j <= lastinrange; j++) {
        double tick = gts.ticks[j];
        double value = ((Number) GTSHelper.valueAtIndex(gts, j)).doubleValue();
        double area = 0.5D * Math.abs(((reftick - tickavg) * (value - refvalue)) - (reftick - tick) * (valueavg - refvalue)); 
               
        if (area > maxarea) {
          maxarea = area;
          nextref = j;
        }
      }
      
      GTSHelper.setValue(sampled, GTSHelper.tickAtIndex(gts, nextref), GTSHelper.locationAtIndex(gts, nextref), GTSHelper.elevationAtIndex(gts, nextref), GTSHelper.valueAtIndex(gts, nextref), false);
    }
    
    // Add last datapoint
    GTSHelper.setValue(sampled, GTSHelper.tickAtIndex(gts,gts.values - 1), GTSHelper.locationAtIndex(gts, gts.values - 1), GTSHelper.elevationAtIndex(gts, gts.values - 1), GTSHelper.valueAtIndex(gts, gts.values - 1), false);
    
    return sampled;
  }
}