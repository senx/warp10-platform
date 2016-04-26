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

package io.warp10.script;


import com.google.common.collect.Lists;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Export the FULL functions list slitted into these categories
 * Function
 * operator (comparaison / arithemtic / logical )
 * Constants
 * frameworks (bucketize / map / reduce / apply )
 *
 * Adds stack structures <% %> <S S> [] {}
 */
public class ExportFunctions {

  private static String CONSTANTS = "constants";
  private static String OP_ARITMETIC = "operator.artimetic";
  private static String OP_LOGICAL = "operator.logical";
  private static String OP_COMPARAISON = "operator.comparison";
  private static String OP_BITWISE = "operator.bitwise";

  private static String FCT_MATH = "functions.math";
  private static String FCT_TIMEUNIT = "functions.timeunit";
  private static String FCT_TRIGO = "functions.trigonometry";
  private static String FCT_DATE = "functions.date";
  private static String FCT_STRING = "functions.string";
  private static String FCT_LIST = "functions.list";
  private static String FCT_STACK = "functions.stack";
  private static String FCT_GTS = "functions.gts";
  private static String FCT_LOGIC_STRUCTURE = "functions.logicStructures";
  private static String FCT_PLATFORM = "functions.platform";
  private static String FCT_OUTLIER = "functions.outlier";
  private static String FCT_BUCKETIZED  = "functions.bucketized";
  private static String FCT_GEO  = "functions.geo";
  private static String FCT_TYPE_CONVERSION  = "functions.typeConversion";

  private static String MACROS = "macros";

  public static void main(String[] args) throws Exception {
    List<String> functionsFullList = new ArrayList<>();
    try {
      // THIS is a very bad thing, used only for extraction purpose

      // Extract java functions
      Field field = JavaLibrary.class.getDeclaredField("functions");
      field.setAccessible(true);
      Map<String,Object> functions  = (Map<String,Object>) field.get(null);
      functionsFullList.addAll(functions.keySet());

      // Extract WarpScript functions
      Field warpScriptField = WarpScriptLib.class.getDeclaredField("functions");
      warpScriptField.setAccessible(true);
      functions  = (Map<String,Object>) warpScriptField.get(null);
      functionsFullList.addAll(functions.keySet());
    } catch (Exception exp) {
      exp.printStackTrace();
    }

    // -------------------------------------------------------------
    // output functions by category
    // -------------------------------------------------------------
    Map<String, List<String>> frameworksFunctions = new HashMap<>();
    frameworksFunctions.put("MAP", new ArrayList<String>());
    frameworksFunctions.put("REDUCE", new ArrayList<String>());
    frameworksFunctions.put("APPLY", new ArrayList<String>());
    frameworksFunctions.put("FILTER", new ArrayList<String>());
    frameworksFunctions.put("BUCKETIZE", new ArrayList<String>());

    Map<String, List<String>> operators = new HashMap<>();
    //functions.put(CONSTANTS, new ArrayList<String>());
    operators.put(OP_ARITMETIC, new ArrayList<String>());
    operators.put(OP_LOGICAL, new ArrayList<String>());
    operators.put(OP_COMPARAISON, new ArrayList<String>());
    operators.put(OP_BITWISE, new ArrayList<String>());

    Map<String, List<String>> functions = new HashMap<>();
    functions.put(FCT_MATH, new ArrayList<String>());
    functions.put(FCT_TIMEUNIT, new ArrayList<String>());
    functions.put(FCT_TRIGO, new ArrayList<String>());
    functions.put(FCT_DATE, new ArrayList<String>());
    functions.put(FCT_STRING, new ArrayList<String>());
    functions.put(FCT_LIST, new ArrayList<String>());
    functions.put(FCT_STACK, new ArrayList<String>());
    functions.put(FCT_LOGIC_STRUCTURE, new ArrayList<String>());
    functions.put(FCT_PLATFORM, new ArrayList<String>());
    functions.put(FCT_GTS, new ArrayList<String>());
    functions.put(FCT_OUTLIER, new ArrayList<String>());
    functions.put(FCT_BUCKETIZED, new ArrayList<String>());
    functions.put(FCT_GEO, new ArrayList<String>());
    functions.put(FCT_TYPE_CONVERSION, new ArrayList<String>());

    List<String> constants = new ArrayList<String>();
    List<String> macros = new ArrayList<String>();

    // -------------------------------------------------------------
    // patterns
    // -------------------------------------------------------------
    Pattern mapPattern = Pattern.compile("mapper\\..*");
    Pattern reducePattern = Pattern.compile("reducer\\..*");
    Pattern bucketizePattern = Pattern.compile("bucketizer\\..*");
    Pattern applyPattern = Pattern.compile("op\\..*");
    Pattern filterPattern = Pattern.compile("filter\\..*");
    Pattern processingPattern = Pattern.compile("P[a-z].*");
    Pattern macroPattern = Pattern.compile("MACRO.*");


    // STATIC CATEGORISATION
    List<String> frameworks = Lists.newArrayList("MAP", "REDUCE", "BUCKETIZE", "APPLY", "FILTER");
    List<String> structure = Lists.newArrayList("[", "]", "{", "}", "<%", "%>", "<S", "S>", "<'", "'>");

    // OPERATORS
    Map<String, List<String>> staticOperators = new HashMap<>();
    staticOperators.put(OP_ARITMETIC, Lists.newArrayList("+", "-", "*", "**", "/", "%"));
    staticOperators.put(OP_LOGICAL, Lists.newArrayList("&&", "||", "!", "AND", "OR", "NOT"));
    staticOperators.put(OP_COMPARAISON, Lists.newArrayList("==", "~=", "!=", "<=", ">=", "<", ">"));
    staticOperators.put(OP_BITWISE, Lists.newArrayList("<<", ">>", ">>>", "&", "|", "^", "~"));

    // CONSTS
    List<String> staticConstants = Lists.newArrayList("E", "e","MAXLONG","MINLONG","NaN","NULL","PI","pi", "max.time.sliding.window", "max.tick.sliding.window");

    List<String> staticMacros = Lists.newArrayList("MACROMAPPER","MACROREDUCER","MACROFILTER","STRICTMAPPER","MACROBUCKETIZER");

    // FUNCTIONS
    Map<String, List<String>> staticFunctions = new HashMap<>();
    staticFunctions.put(FCT_MATH, Lists.newArrayList("ABS","CBRT","CEIL","EXP","FLOOR","IEEEREMAINDER","LBOUNDS","LOG","LOG10","MAX","MIN","NBOUNDS","NEXTAFTER","NEXTUP","NPDF","PROBABILITY","RAND","REVBITS","RINT","ROUND","SIGNUM","SQRT"));
    staticFunctions.put(FCT_TIMEUNIT, Lists.newArrayList("w","d","h","m","s", "ms","us", "ns", "ps"));
    staticFunctions.put(FCT_TRIGO, Lists.newArrayList("COS","COSH","ACOS","SIN","SINH","ASIN","TAN","TANH","ATAN","TODEGREES","TORADIANS"));
    staticFunctions.put(FCT_DATE, Lists.newArrayList("DURATION","ISO8601","MSTU","NOW","STU","TSELEMENTS"));
    staticFunctions.put(FCT_STRING, Lists.newArrayList("->HEX","B64TOHEX","B64->","B64URL->","BINTOHEX","FROMBIN","FROMBITS","FROMHEX","HASH","HEX->","HEXTOB64","HEXTOBIN","JOIN","MATCH","MATCHER","SPLIT","SUBSTRING","TEMPLATE","->B64URL","->B64","TOBIN","TOBITS","TOHEX","TOLOWER","TOUPPER","TRIM","URLDECODE","URLENCODE","UUID"));
    staticFunctions.put(FCT_LIST, Lists.newArrayList("[]","{}","{","}","[","]","->LIST","->MAP","APPEND","CLONEREVERSE","CONTAINSKEY","CONTAINS","CONTAINSVALUE","FLATTEN","GET","KEYLIST","LFLATMAP","LIST->","LMAP","LSORT","MAP->","MSORT","PUT","REMOVE","REVERSE","SET","SIZE","SUBLIST","SUBMAP","UNIQUE","VALUELIST","ZIP"));
    staticFunctions.put(FCT_LOGIC_STRUCTURE, Lists.newArrayList("ISNaN","ISNULL","ASSERT","BREAK","CONTINUE","DEFINED","EVAL","FAIL","FOREACH","FOR","FORSTEP","IFTE","IFT","MSGFAIL","NRETURN","RETURN","STOP","SWITCH","UNTIL","WHILE"));
    staticFunctions.put(FCT_PLATFORM, Lists.newArrayList("EVALSECURE","IDENT","JSONLOOSE","JSONSTRICT","LIMIT","MAXBUCKETS","MAXDEPTH","MAXGTS","MAXLOOP","MAXOPS","MAXSYMBOLS","NOOP","OPS","RESTORE","REV","SAVE","SECUREKEY","TOKENINFO","UNSECURE","URLFETCH","WEBCALL"));
    staticFunctions.put(FCT_GTS, Lists.newArrayList("ADDVALUE","ATINDEX","ATTICK","BBOX","CLONE","CLONEEMPTY","COMMONTICKS","COMPACT","CORRELATE","DEDUP","ELEVATIONS","FETCH","FETCHBOOLEAN","FETCHDOUBLE","FETCHLONG","FETCHSTRING","FILLTICKS","FIND","FIRSTTICK","INTEGRATE","ISONORMALIZE","LABELS","LASTSORT","LASTTICK","LOCATIONS","LOWESS","MERGE","META","METASORT","MUSIGMA","NAME","NEWGTS","NONEMPTY","NORMALIZE","NSUMSUMSQ","PARSESELECTOR","QUANTIZE","RANGECOMPACT","RELABEL","RENAME","RESETS","RLOWESS","RSORT","SHRINK","SINGLEEXPONENTIALSMOOTHING","SORT","STANDARDIZE","TICKINDEX","TICKLIST","TICKS","TIMECLIP","TIMEMODULO","TIMESCALE","TIMESHIFT","TIMESPLIT","TOSELECTOR","UNWRAP","UPDATE","VALUES","WRAP","ZSCORE"));
    staticFunctions.put(FCT_OUTLIER, Lists.newArrayList("THRESHOLDTEST","ZSCORETEST","GRUBBSTEST","ESDTEST","STLESDTEST","HYBRIDTEST","HYBRIDTEST2"));
    staticFunctions.put(FCT_BUCKETIZED, Lists.newArrayList("ATBUCKET","BUCKETCOUNT","BUCKETSPAN","CROP","FILLNEXT","FILLPREVIOUS","FILLVALUE","INTERPOLATE","LASTBUCKET","STL","UNBUCKETIZE"));
    staticFunctions.put(FCT_GEO, Lists.newArrayList("GEO.DIFFERENCE","GEO.INTERSECTION","GEO.INTERSECTS","GEO.UNION","GEO.WITHIN","GEO.WKT","HAVERSINE"));
    staticFunctions.put(FCT_TYPE_CONVERSION, Lists.newArrayList("->JSON","JSON->","TODOUBLE","TOLONG","TOSTRING","TOTIMESTAMP"));
    staticFunctions.put(FCT_STACK, Lists.newArrayList("AUTHENTICATE","BOOTSTRAP","COUNTTOMARK","CLEAR","CLEARTOMARK","CSTORE","DEF","DEPTH","DEBUGON","DEBUGOFF","DOC","DOCMODE","DROP","DROPN","DUP","DUPN","EXPORT","FORGET","LOAD","MARK","NDEBUGON","PICK","ROLL","ROLLD","ROT","RUN","STORE","SWAP","TYPEOF"));


    /*






     */

    // Sort all functions list
    for (String function: functionsFullList) {
      // exclude frameworks
      if (frameworks.contains(function)) {
        continue;
      }

      // exclude processing
      if (processingPattern.matcher(function).matches()) {
        continue;
      }

      // -----------------------------------------------
      // test frameworks patterns
      // -----------------------------------------------
      if (mapPattern.matcher(function).matches()) {
        frameworksFunctions.get("MAP").add(function);
        continue;
      }

      if (reducePattern.matcher(function).matches()) {
        frameworksFunctions.get("REDUCE").add(function);
        continue;
      }

      if (bucketizePattern.matcher(function).matches()) {
        frameworksFunctions.get("BUCKETIZE").add(function);
        continue;
      }

      if (applyPattern.matcher(function).matches()) {
        frameworksFunctions.get("APPLY").add(function);
        continue;
      }

      if (filterPattern.matcher(function).matches()) {
        frameworksFunctions.get("FILTER").add(function);
        continue;
      }

      boolean functionMatch = false;

      // ---------------------------------------------
      // Values with static categorisation (constants + macros)
      // ---------------------------------------------
      if (staticConstants.contains(function)) {
        constants.add(function);
        continue;
      }

      if (staticMacros.contains(function)) {
        macros.add(function);
        continue;
      }

      // ---------------------------------------------
      // Values with static categorisation (operators)
      // ---------------------------------------------
      for (Map.Entry<String, List<String>> entry : staticOperators.entrySet()) {
        // match
        if (entry.getValue().contains(function)) {
          // add if to the key
          operators.get(entry.getKey()).add(function);
          functionMatch = true;
          break;
        }
      }

      if (functionMatch) {
        continue;
      }

      // ---------------------------------------------
      // Values with static categorisation (functions)
      // ---------------------------------------------
      for (Map.Entry<String, List<String>> entry : staticFunctions.entrySet()) {
        // match
        if (entry.getValue().contains(function)) {
          // add if to the key
          functions.get(entry.getKey()).add(function);
          functionMatch = true;
          break;
        }
      }

      if (functionMatch) {
        continue;
      }

      System.out.println(function);

    }

    // build output json object
    Map<String, Object> output = new HashMap<>();

    output.put("frameworks", frameworksFunctions);
    output.put("operators", operators);
    output.put("functions", functions);
    output.put("constants", constants);
    output.put("macros", macros);

    ObjectMapper jsonOutput =  JsonFactory.create();
    System.out.println(jsonOutput.toJson(output));
  }

}
