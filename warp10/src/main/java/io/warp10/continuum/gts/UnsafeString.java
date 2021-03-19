//
//   Copyright 2018  SenX S.A.S.
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

/**
 * The following class is inspired by:
 * 
 * @see <a href="https://raw.githubusercontent.com/nitsanw/javanetperf/psylobsaw/src/psy/lob/saw/UnsafeString.java">https://raw.githubusercontent.com/nitsanw/javanetperf/psylobsaw/src/psy/lob/saw/UnsafeString.java</a>
 * @see <a href="http://psy-lob-saw.blogspot.fr/2012/12/encode-utf-8-string-to-bytebuffer-faster.html">http://psy-lob-saw.blogspot.fr/2012/12/encode-utf-8-string-to-bytebuffer-faster.html</a>
 * 
 */

public class UnsafeString {  
    
  public final static String[] split(String s, char ch) {
        
    //ArrayList<String> tokens = new ArrayList<String>();
    
    int off = 0;
    int next = 0;
    
    //
    // Count the number of separators
    //
    
    int n = 1;
    
    while((next = s.indexOf(ch,off)) != -1) {
      off = next + 1;
      n++;
    }
    
    next = 0;
    off = 0;

    String[] tokens = new String[n];
    int idx = 0;
    
    while ((next = s.indexOf(ch, off)) != -1) {
      //tokens.add(substring(s, off, next));
      //tokens.add(s.substring(off,next));
      tokens[idx++] = s.substring(off,next);
      off = next + 1;
    }
    
    // If no match was found, return this
    if (off == 0) {
      return new String[]{s};
    }
    
    //tokens.add(substring(s, off, s.length()));
    //tokens.add(s.substring(off));
    tokens[idx] = s.substring(off);

    //return tokens.toArray(new String[0]);
    return tokens;
  }
  
  public static boolean isLong(String s) {
    //
    // Skip leading whitespaces
    //
    
    int i = 0;
    
    while(i < s.length() && ' ' == s.charAt(i)) {
      i++;
    }
    
    if (i == s.length()) {
      return false;
    }
    
    //
    // Check sign
    //
    
    char ch = s.charAt(i);
    
    if ('+' != ch && '-' != ch && (ch < '0' || ch > '9')) {
      return false;
    } else if ('-' == ch || '+' == ch) {
      i++;
    }
    
    boolean hasDigits = false;
    
    while(i < s.length()) {
      ch = s.charAt(i);
      if (ch < '0' || ch > '9') {
        return false;
      }
      hasDigits = true;
      i++;
    }

    if (!hasDigits) {
      return false;
    }
  
    return true;
  }
    
  public static boolean isDouble(String s) {
    //
    // Skip leading whitespaces
    //
    
    int i = 0;
    
    while(i < s.length() && ' ' == s.charAt(i)) {
      i++;
    }
    
    if (i == s.length()) {
      return false;
    }
    
    //
    // Check sign
    //

    char ch = s.charAt(i);
    
    if ('-' == ch || '+' == ch) {
      i++;
      if (i >= s.length()) {
        return false;
      }
    }
        
    //
    // Handle NaN
    //
    
    ch = s.charAt(i);
    
    if (3 == s.length() - i) {
      if ('N' == ch && 'a' == s.charAt(i + 1) && 'N' == s.charAt(i + 2)) {
        return true;
      }
    }

    //
    // Handle Infinity
    //
    
    if (8 == s.length() - i) {
      if ('I' == ch && 'n' == s.charAt(i+1) && 'f' == s.charAt(i+2) && 'i' == s.charAt(i+3) && 'n' == s.charAt(i+4) && 'i' == s.charAt(i+5) && 't' == s.charAt(i+6) && 'y' == s.charAt(i+7)) {
        return true;
      }
    }
    
    //
    // If the next char is not a digit, exit
    //
    
    if (ch < '0' || ch > '9') {
      return false;
    }

    int ne = 0;
    int nsign = 0;
    int ndot = 0;
    
    while(i < s.length()) {
      ch = s.charAt(i);
      if (ch >= '0' && ch <= '9') {
        i++;
        continue;
      }
      
      if ('e' == ch || 'E' == ch) {
        if (0 != nsign) {
          // 'e' MUST appear before the sign
          return false;
        }
        ne++;
        i++;
        continue;
      }
      
      if ('-' == ch || '+' == ch) {
        if (0 == ne) {
          // We MUST have encountered an 'e' prior to a sign
          return false;
        }
        nsign++;
        i++;
        continue;
      }
      
      if ('.' == ch) {
        // The dot cannot appear after 'e' or the exponent sign
        if (0 != ne || 0 != nsign) {
          return false;
        }
        ndot++;
        i++;
        continue;
      }      
      return false;
    }
    
    //
    // If we encountered more than one 'e', sign or dot, return false
    //
    
    if (ne > 1 || nsign > 1 || ndot > 1) {
      return false;
    }
    
    //
    // If we encountered neither dot nor 'e', return false
    //
    
    if (0 == ne && 0 == ndot) {
      return false;
    }
    
    return true;    
  }
  
  /**
   * Checks if the given string could be a decimal double, i.e. only contains '.', '-', '+', '0'-'9'.
   * It does not check if the string is actually a valid double or not, just what kind of double it
   * would be.
   */
  public static boolean mayBeDecimalDouble(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if ((c < '0' || c > '9') && '.' != c && '+' != c && '-' != c) {
        return false;
      }
    }
    
    return true;
  }
    
  /**
   * Replace whitespaces in Strings enclosed in single quotes
   * with '%20'
   */
  public static String sanitizeStrings(String str) {
    int idx = 0;
    
    String newstr = str;
    StringBuilder sb = null;
    boolean instring = false;
    char stringsep = '\0';
    
    int lastidx = 0;
    
    while (idx < str.length()) {
      
      char ch = str.charAt(idx);
      
      if (instring && stringsep == ch) {
        instring = false;
        stringsep = '\0';
      } else if (!instring) {
        if ('\'' == ch) {
          instring = true; 
          stringsep = '\'';
        } else if ('\"' == ch) {
          instring = true;
          stringsep = '\"';
        }
      }
            
      if (instring) {
        if (' ' == ch) {
          if (null == sb) {
            // This is the first space in a string we encounter, allocate a StringBuilder
            // and copy the prefix into it
            sb = new StringBuilder();
          }
          // We encountered a whitespace, copy everything since lastidx
          for (int offset = 0; offset < idx - lastidx; offset++) {
            sb.append(str.charAt(lastidx + offset));
          }
          sb.append("%20");
          lastidx = idx + 1;
        }
      }
      
      idx++;
    }
    
    if (null != sb) {
      if (lastidx < str.length()) {
        for (int offset = 0; offset < str.length() - lastidx; offset++) {
          sb.append(str.charAt(lastidx + offset));
        }
      }
      newstr = sb.toString();
    }
    
    return newstr;
  }  
}
