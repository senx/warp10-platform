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

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.CORRELATE;
import io.warp10.continuum.gts.DISCORDS;
import io.warp10.continuum.gts.FFT;
import io.warp10.continuum.gts.IFFT;
import io.warp10.continuum.gts.INTERPOLATE;
import io.warp10.continuum.gts.LOCATIONOFFSET;
import io.warp10.continuum.gts.ZIP;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.*;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.aggregator.Percentile;
import io.warp10.script.binary.ADD;
import io.warp10.script.binary.BitwiseAND;
import io.warp10.script.binary.BitwiseOR;
import io.warp10.script.binary.BitwiseXOR;
import io.warp10.script.binary.CondAND;
import io.warp10.script.binary.CondOR;
import io.warp10.script.binary.DIV;
import io.warp10.script.binary.EQ;
import io.warp10.script.binary.GE;
import io.warp10.script.binary.GT;
import io.warp10.script.binary.LE;
import io.warp10.script.binary.LT;
import io.warp10.script.binary.MOD;
import io.warp10.script.binary.MUL;
import io.warp10.script.binary.NE;
import io.warp10.script.binary.POW;
import io.warp10.script.binary.SHIFTLEFT;
import io.warp10.script.binary.SHIFTRIGHT;
import io.warp10.script.binary.SUB;
import io.warp10.script.filter.FilterByClass;
import io.warp10.script.filter.FilterByLabels;
import io.warp10.script.filter.FilterByMetadata;
import io.warp10.script.filter.FilterLastEQ;
import io.warp10.script.filter.FilterLastGE;
import io.warp10.script.filter.FilterLastGT;
import io.warp10.script.filter.FilterLastLE;
import io.warp10.script.filter.FilterLastLT;
import io.warp10.script.filter.FilterLastNE;
import io.warp10.script.filter.LatencyFilter;
import io.warp10.script.functions.*;
import io.warp10.script.lora.LORAENC;
import io.warp10.script.lora.LORAMIC;
import io.warp10.script.mapper.MapperAdd;
import io.warp10.script.mapper.MapperDayOfMonth;
import io.warp10.script.mapper.MapperDayOfWeek;
import io.warp10.script.mapper.MapperDotProduct;
import io.warp10.script.mapper.MapperDotProductPositive;
import io.warp10.script.mapper.MapperDotProductSigmoid;
import io.warp10.script.mapper.MapperDotProductTanh;
import io.warp10.script.mapper.MapperExp;
import io.warp10.script.mapper.MapperGeoApproximate;
import io.warp10.script.mapper.MapperGeoOutside;
import io.warp10.script.mapper.MapperGeoWithin;
import io.warp10.script.mapper.MapperHourOfDay;
import io.warp10.script.mapper.MapperKernelCosine;
import io.warp10.script.mapper.MapperKernelEpanechnikov;
import io.warp10.script.mapper.MapperKernelGaussian;
import io.warp10.script.mapper.MapperKernelLogistic;
import io.warp10.script.mapper.MapperKernelQuartic;
import io.warp10.script.mapper.MapperKernelSilverman;
import io.warp10.script.mapper.MapperKernelTriangular;
import io.warp10.script.mapper.MapperKernelTricube;
import io.warp10.script.mapper.MapperKernelTriweight;
import io.warp10.script.mapper.MapperKernelUniform;
import io.warp10.script.mapper.MapperLog;
import io.warp10.script.mapper.MapperMaxX;
import io.warp10.script.mapper.MapperMinX;
import io.warp10.script.mapper.MapperMinuteOfHour;
import io.warp10.script.mapper.MapperMonthOfYear;
import io.warp10.script.mapper.MapperMul;
import io.warp10.script.mapper.MapperNPDF;
import io.warp10.script.mapper.MapperPow;
import io.warp10.script.mapper.MapperReplace;
import io.warp10.script.mapper.MapperSecondOfMinute;
import io.warp10.script.mapper.MapperTick;
import io.warp10.script.mapper.MapperYear;
import io.warp10.script.mapper.STRICTMAPPER;
import io.warp10.script.processing.Pencode;
import io.warp10.script.processing.color.Palpha;
import io.warp10.script.processing.color.Pbackground;
import io.warp10.script.processing.color.Pblue;
import io.warp10.script.processing.color.Pbrightness;
import io.warp10.script.processing.color.Pclear;
import io.warp10.script.processing.color.Pcolor;
import io.warp10.script.processing.color.PcolorMode;
import io.warp10.script.processing.color.Pfill;
import io.warp10.script.processing.color.Pgreen;
import io.warp10.script.processing.color.Phue;
import io.warp10.script.processing.color.PlerpColor;
import io.warp10.script.processing.color.PnoFill;
import io.warp10.script.processing.color.PnoStroke;
import io.warp10.script.processing.color.Pred;
import io.warp10.script.processing.color.Psaturation;
import io.warp10.script.processing.color.Pstroke;
import io.warp10.script.processing.image.Pblend;
import io.warp10.script.processing.image.Pcopy;
import io.warp10.script.processing.image.Pdecode;
import io.warp10.script.processing.image.Pget;
import io.warp10.script.processing.image.Pimage;
import io.warp10.script.processing.image.PimageMode;
import io.warp10.script.processing.image.PnoTint;
import io.warp10.script.processing.image.Ppixels;
import io.warp10.script.processing.image.Pset;
import io.warp10.script.processing.image.Ptint;
import io.warp10.script.processing.image.PupdatePixels;
import io.warp10.script.processing.math.Pconstrain;
import io.warp10.script.processing.math.Pdist;
import io.warp10.script.processing.math.Plerp;
import io.warp10.script.processing.math.Pmag;
import io.warp10.script.processing.math.Pmap;
import io.warp10.script.processing.math.Pnorm;
import io.warp10.script.processing.rendering.PGraphics;
import io.warp10.script.processing.rendering.PblendMode;
import io.warp10.script.processing.rendering.Pclip;
import io.warp10.script.processing.rendering.PnoClip;
import io.warp10.script.processing.shape.Parc;
import io.warp10.script.processing.shape.PbeginContour;
import io.warp10.script.processing.shape.PbeginShape;
import io.warp10.script.processing.shape.Pbezier;
import io.warp10.script.processing.shape.PbezierDetail;
import io.warp10.script.processing.shape.PbezierPoint;
import io.warp10.script.processing.shape.PbezierTangent;
import io.warp10.script.processing.shape.PbezierVertex;
import io.warp10.script.processing.shape.Pbox;
import io.warp10.script.processing.shape.Pcurve;
import io.warp10.script.processing.shape.PcurveDetail;
import io.warp10.script.processing.shape.PcurvePoint;
import io.warp10.script.processing.shape.PcurveTangent;
import io.warp10.script.processing.shape.PcurveTightness;
import io.warp10.script.processing.shape.PcurveVertex;
import io.warp10.script.processing.shape.Pellipse;
import io.warp10.script.processing.shape.PellipseMode;
import io.warp10.script.processing.shape.PendContour;
import io.warp10.script.processing.shape.PendShape;
import io.warp10.script.processing.shape.Pline;
import io.warp10.script.processing.shape.Ppoint;
import io.warp10.script.processing.shape.Pquad;
import io.warp10.script.processing.shape.PquadraticVertex;
import io.warp10.script.processing.shape.Prect;
import io.warp10.script.processing.shape.PrectMode;
import io.warp10.script.processing.shape.Psphere;
import io.warp10.script.processing.shape.PsphereDetail;
import io.warp10.script.processing.shape.PstrokeCap;
import io.warp10.script.processing.shape.PstrokeJoin;
import io.warp10.script.processing.shape.PstrokeWeight;
import io.warp10.script.processing.shape.Ptriangle;
import io.warp10.script.processing.shape.Pvertex;
import io.warp10.script.processing.structure.PpopStyle;
import io.warp10.script.processing.structure.PpushStyle;
import io.warp10.script.processing.transform.PpopMatrix;
import io.warp10.script.processing.transform.PpushMatrix;
import io.warp10.script.processing.transform.PresetMatrix;
import io.warp10.script.processing.transform.Protate;
import io.warp10.script.processing.transform.ProtateX;
import io.warp10.script.processing.transform.ProtateY;
import io.warp10.script.processing.transform.ProtateZ;
import io.warp10.script.processing.transform.Pscale;
import io.warp10.script.processing.transform.PshearX;
import io.warp10.script.processing.transform.PshearY;
import io.warp10.script.processing.transform.Ptranslate;
import io.warp10.script.processing.typography.PcreateFont;
import io.warp10.script.processing.typography.Ptext;
import io.warp10.script.processing.typography.PtextAlign;
import io.warp10.script.processing.typography.PtextAscent;
import io.warp10.script.processing.typography.PtextDescent;
import io.warp10.script.processing.typography.PtextFont;
import io.warp10.script.processing.typography.PtextLeading;
import io.warp10.script.processing.typography.PtextMode;
import io.warp10.script.processing.typography.PtextSize;
import io.warp10.script.processing.typography.PtextWidth;
import io.warp10.script.unary.ABS;
import io.warp10.script.unary.COMPLEMENT;
import io.warp10.script.unary.FROMBIN;
import io.warp10.script.unary.FROMBITS;
import io.warp10.script.unary.FROMHEX;
import io.warp10.script.unary.NOT;
import io.warp10.script.unary.REVERSEBITS;
import io.warp10.script.unary.TOBIN;
import io.warp10.script.unary.TOBITS;
import io.warp10.script.unary.TOBOOLEAN;
import io.warp10.script.unary.TODOUBLE;
import io.warp10.script.unary.TOHEX;
import io.warp10.script.unary.TOLONG;
import io.warp10.script.unary.TOSTRING;
import io.warp10.script.unary.TOTIMESTAMP;
import io.warp10.script.unary.UNIT;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Library of functions used to manipulate Geo Time Series
 * and more generally interact with an EinsteinStack
 */
public class WarpScriptLib {
  private static Map<String,WarpScriptStackFunction> functions = new HashMap<String, WarpScriptStackFunction>();
  
  /**
   * Static definition of name so it can be reused outside of EinsteinLib
   */
  public static final String EVAL = "EVAL";
  public static final String LOAD = "LOAD";
  public static final String RUN = "RUN";
  public static final String BOOTSTRAP = "BOOTSTRAP";
  
  public static final String GEO_WKT = "GEO.WKT";
  public static final String GEO_INTERSECTION = "GEO.INTERSECTION";
  public static final String GEO_DIFFERENCE = "GEO.DIFFERENCE";
  public static final String GEO_UNION = "GEO.UNION";
  
  static {
    
    functions.put("REV", new REV("REV"));
    
    functions.put(BOOTSTRAP, new NOOP(BOOTSTRAP));
    
    //
    // Stack manipulation functions
    //
    
    functions.put("MARK", new MARK("MARK"));
    functions.put("CLEARTOMARK", new CLEARTOMARK("CLEARTOMARK"));
    functions.put("COUNTTOMARK", new COUNTTOMARK("COUNTTOMARK"));
    functions.put("AUTHENTICATE", new AUTHENTICATE("AUTHENTICATE"));
    functions.put("STACKATTRIBUTE", new STACKATTRIBUTE("STACKATTRIBUTE")); // NOT TO BE DOCUMENTED
    functions.put("EXPORT", new EXPORT("EXPORT"));
    functions.put("TIMINGS", new TIMINGS("TIMINGS")); // NOT TO BE DOCUMENTED (YET)
    functions.put("NOTIMINGS", new NOTIMINGS("NOTIMINGS")); // NOT TO BE DOCUMENTED (YET)
    functions.put("ELAPSED", new ELAPSED("ELAPSED")); // NOT TO BE DOCUMENTED (YET)
    functions.put("->LIST", new TOLIST("->LIST"));
    functions.put("LIST->", new LISTTO("LIST->"));
    functions.put("->SET", new TOSET("->SET"));
    functions.put("SET->", new SETTO("SET->"));
    functions.put("UNION", new UNION("UNION"));
    functions.put("INTERSECTION", new INTERSECTION("INTERSECTION"));
    functions.put("DIFFERENCE", new DIFFERENCE("DIFFERENCE"));
    functions.put("->MAP", new TOMAP("->MAP"));
    functions.put("MAP->", new MAPTO("MAP->"));
    functions.put("MAPID", new MAPID("MAPID"));
    functions.put("->JSON", new TOJSON("->JSON"));      
    functions.put("JSON->", new JSONTO("JSON->"));
    functions.put("GET", new GET("GET"));
    functions.put("SET", new SET("SET"));
    functions.put("PUT", new PUT("PUT"));
    functions.put("SUBMAP", new SUBMAP("SUBMAP"));
    functions.put("SUBLIST", new SUBLIST("SUBLIST"));
    functions.put("DEVICES", new DEVICES("DEVICES"));
    functions.put("KEYLIST", new KEYLIST("KEYLIST"));
    functions.put("VALUELIST", new VALUELIST("VALUELIST"));
    functions.put("SIZE", new SIZE("SIZE"));
    functions.put("SHRINK", new SHRINK("SHRINK"));
    functions.put("REMOVE", new REMOVE("REMOVE"));
    functions.put("UNIQUE", new UNIQUE("UNIQUE"));
    functions.put("CONTAINS", new CONTAINS("CONTAINS"));
    functions.put("CONTAINSKEY", new CONTAINSKEY("CONTAINSKEY"));
    functions.put("CONTAINSVALUE", new CONTAINSVALUE("CONTAINSVALUE"));
    functions.put("REVERSE", new REVERSE("REVERSE", true));
    functions.put("CLONEREVERSE", new REVERSE("CLONEREVERSE", false));
    functions.put("DUP", new DUP("DUP"));
    functions.put("DUPN", new DUPN("DUPN"));
    functions.put("SWAP", new SWAP("SWAP"));
    functions.put("DROP", new DROP("DROP"));
    functions.put("SAVE", new SAVE("SAVE"));
    functions.put("RESTORE", new RESTORE("RESTORE"));
    functions.put("CLEAR", new CLEAR("CLEAR"));
    functions.put("CLEARDEFS", new CLEARDEFS("CLEARDEFS"));
    functions.put("CLEARSYMBOLS", new CLEARSYMBOLS("CLEARSYMBOLS"));
    functions.put("DROPN", new DROPN("DROPN"));
    functions.put("ROT", new ROT("ROT"));
    functions.put("ROLL", new ROLL("ROLL"));
    functions.put("ROLLD", new ROLLD("ROLLD"));
    functions.put("PICK", new PICK("PICK"));
    functions.put("DEPTH", new DEPTH("DEPTH"));
    functions.put("MAXDEPTH", new MAXDEPTH("MAXDEPTH"));
    functions.put("RESET", new RESET("RESET"));
    functions.put("MAXOPS", new MAXOPS("MAXOPS"));
    functions.put("MAXLOOP", new MAXLOOP("MAXLOOP"));
    functions.put("MAXBUCKETS", new MAXBUCKETS("MAXBUCKETS"));
    functions.put("MAXPIXELS", new MAXPIXELS("MAXPIXELS"));
    functions.put("OPS", new OPS("OPS"));
    functions.put("MAXSYMBOLS", new MAXSYMBOLS("MAXSYMBOLS"));
    functions.put(EVAL, new EVAL(EVAL));
    functions.put("NOW", new NOW("NOW"));
    functions.put("MSTU", new MSTU("MSTU"));
    functions.put("STU", new STU("STU"));
    functions.put("UNIXTIMEEND", new UNIXTIMEEND("UNIXTIMEEND"));
    functions.put("UNIXTIMEALIGN", new UNIXTIMEALIGN("UNIXTIMEALIGN"));
    functions.put("APPEND", new APPEND("APPEND"));
    functions.put("STORE", new STORE("STORE"));
    functions.put("CSTORE", new CSTORE("CSTORE"));
    functions.put(LOAD, new LOAD(LOAD));
    functions.put(RUN, new RUN(RUN));
    functions.put("DEF", new DEF("DEF"));
    functions.put("UDF", new UDF("UDF", false));
    functions.put("CUDF", new UDF("CUDF", true));
    functions.put("FORGET", new FORGET("FORGET"));    
    functions.put("DEFINED", new DEFINED("DEFINED"));
    functions.put("NaN", new NaN("NaN"));
    functions.put("ISNaN", new ISNaN("ISNaN"));
    functions.put("TYPEOF", new TYPEOF("TYPEOF"));      
    functions.put("ASSERT", new ASSERT("ASSERT"));
    functions.put("FAIL", new FAIL("FAIL"));
    functions.put("MSGFAIL", new MSGFAIL("MSGFAIL"));
    functions.put("STOP", new STOP("STOP"));
    functions.put("JSONSTRICT", new JSONSTRICT("JSONSTRICT"));
    functions.put("JSONLOOSE", new JSONLOOSE("JSONLOOSE"));
    functions.put("DEBUGON", new DEBUGON("DEBUGON"));
    functions.put("NDEBUGON", new NDEBUGON("NDEBUGON"));
    functions.put("DEBUGOFF", new DEBUGOFF("DEBUGOFF"));
    functions.put("LMAP", new LMAP("LMAP"));
    functions.put("LFLATMAP", new LFLATMAP("LFLATMAP"));
    functions.put("[]", new EMPTYLIST("[]"));
    functions.put("[", new MARK("["));
    functions.put("]", new ENDLIST("]"));
    functions.put("{}", new EMPTYMAP("{}"));
    functions.put("IMMUTABLE", new IMMUTABLE("IMMUTABLE"));
    functions.put("{", new MARK("{"));
    functions.put("}", new ENDMAP("}"));
    functions.put("SECUREKEY", new SECUREKEY("SECUREKEY"));
    functions.put("UNSECURE", new UNSECURE("UNSECURE", true));
    functions.put("EVALSECURE", new EVALSECURE("EVALSECURE"));
    functions.put("NOOP", new NOOP("NOOP"));
    functions.put("DOC", new DOC("DOC"));
    functions.put("DOCMODE", new DOCMODE("DOCMODE"));
       
    functions.put("MACROMAPPER", new MACROMAPPER("MACROMAPPER"));
    functions.put("MACROREDUCER", new MACROMAPPER("MACROREDUCER"));
    functions.put("MACROBUCKETIZER", new MACROMAPPER("MACROBUCKETIZER"));
    functions.put("MACROFILTER", new MACROFILTER("MACROFILTER"));
    functions.put("STRICTMAPPER", new STRICTMAPPER("STRICTMAPPER"));
    
    functions.put("PARSESELECTOR", new PARSESELECTOR("PARSESELECTOR"));
    functions.put("TOSELECTOR", new TOSELECTOR("TOSELECTOR"));
    
    // Binary ops
    functions.put("+", new ADD("+"));
    functions.put("-", new SUB("-"));
    functions.put("/", new DIV("/"));
    functions.put("*", new MUL("*"));
    functions.put("**", new POW("**"));
    functions.put("%", new MOD("%"));
    functions.put("==", new EQ("=="));
    functions.put("!=", new NE("!="));
    functions.put("<", new LT("<"));
    functions.put(">", new GT(">"));
    functions.put("<=", new LE("<="));
    functions.put(">=", new GE(">="));
    functions.put("&&", new CondAND("&&"));
    functions.put("AND", new CondAND("AND"));
    functions.put("||", new CondOR("||"));
    functions.put("OR", new CondOR("OR"));
    functions.put("&", new BitwiseAND("&"));
    functions.put(">>", new SHIFTRIGHT(">>", true));
    functions.put(">>>", new SHIFTRIGHT(">>>", false));
    functions.put("<<", new SHIFTLEFT("<<"));
    functions.put("|", new BitwiseOR("|"));
    functions.put("^", new BitwiseXOR("^"));
    functions.put("~=", new ALMOSTEQ("~="));

    functions.put("BITGET", new BITGET("BITGET"));
    
    // Unary ops    
    functions.put("!", new NOT("!"));
    functions.put("~", new COMPLEMENT("~"));
    functions.put("REVBITS", new REVERSEBITS("REVBITS"));
    functions.put("NOT", new NOT("NOT"));
    functions.put("ABS", new ABS("ABS"));
    functions.put("TODOUBLE", new TODOUBLE("TODOUBLE"));
    functions.put("TOBOOLEAN", new TOBOOLEAN("TOBOOLEAN"));
    functions.put("TOLONG", new TOLONG("TOLONG"));
    functions.put("TOSTRING", new TOSTRING("TOSTRING"));
    functions.put("TOHEX", new TOHEX("TOHEX"));
    functions.put("TOBIN", new TOBIN("TOBIN"));
    functions.put("FROMHEX", new FROMHEX("FROMHEX"));
    functions.put("FROMBIN", new FROMBIN("FROMBIN"));
    functions.put("TOBITS", new TOBITS("TOBITS"));
    functions.put("FROMBITS", new FROMBITS("FROMBITS"));
    functions.put("TOKENINFO", new TOKENINFO("TOKENINFO"));
    
    // Unit converters
    functions.put("w", new UNIT("w", 7 * 24 * 60 * 60 * 1000));
    functions.put("d", new UNIT("d", 24 * 60 * 60 * 1000));
    functions.put("h", new UNIT("h", 60 * 60 * 1000));
    functions.put("m", new UNIT("m", 60 * 1000));
    functions.put("s",  new UNIT("s",  1000));
    functions.put("ms", new UNIT("ms", 1));
    functions.put("us", new UNIT("us", 0.001));
    functions.put("ns", new UNIT("ns", 0.000001));
    functions.put("ps", new UNIT("ps", 0.000000001));
    
    //
    // String functions
    //
    
    functions.put("URLDECODE", new URLDECODE("URLDECODE"));            // doc/einstein/function_URLDECODE        Example done   Unit test
    functions.put("URLENCODE", new URLENCODE("URLENCODE"));
    functions.put("SPLIT", new SPLIT("SPLIT"));                        // doc/einstein/function_SPLIT            Example done   Unit test
    functions.put("UUID", new UUID("UUID"));
    functions.put("JOIN", new JOIN("JOIN"));
    functions.put("SUBSTRING", new SUBSTRING("SUBSTRING"));
    functions.put("TOUPPER", new TOUPPER("TOUPPER"));
    functions.put("TOLOWER", new TOLOWER("TOLOWER"));
    functions.put("TRIM", new TRIM("TRIM"));
    
    functions.put("B64TOHEX", new B64TOHEX("B64TOHEX"));
    functions.put("HEXTOB64", new HEXTOB64("HEXTOB64"));
    functions.put("BINTOHEX", new BINTOHEX("BINTOHEX"));
    functions.put("HEXTOBIN", new HEXTOBIN("HEXTOBIN"));
    
    functions.put("BIN->", new BINTO("BIN->"));
    functions.put("HEX->", new HEXTO("HEX->"));
    functions.put("B64->", new B64TO("B64->"));
    functions.put("B64URL->", new B64URLTO("B64URL->"));
    functions.put("BYTES->", new BYTESTO("BYTES->"));

    functions.put("->BYTES", new TOBYTES("->BYTES"));
    functions.put("->BIN", new io.warp10.script.functions.TOBIN("->BIN"));
    functions.put("->HEX", new io.warp10.script.functions.TOHEX("->HEX"));
    functions.put("->B64", new TOB64("->B64"));
    functions.put("->B64URL", new TOB64URL("->B64URL"));
    functions.put("->OPB64", new TOOPB64("->OPB64"));
    functions.put("OPB64TOHEX", new OPB64TOHEX("OPB64TOHEX"));    
    
    //
    // Conditionals
    //
    
    functions.put("IFT", new IFT("IFT"));                              // doc/einstein/function_IFT              Example done   Unit test
    functions.put("IFTE", new IFTE("IFTE"));                           // doc/einstein/function_IFTE             Example done   Unit test
    functions.put("SWITCH", new SWITCH("SWITCH"));                     // doc/einstein/function_SWITCH           Example done   Unit test
    
    //
    // Loops
    //
    
    functions.put("WHILE", new WHILE("WHILE"));                        // doc/einstein/function_WHILE            Example done   Unit test
    functions.put("UNTIL", new UNTIL("UNTIL"));                        // doc/einstein/function_UNTIL            Example done   Unit test
    functions.put("FOR", new FOR("FOR"));                              // doc/einstein/function_FOR              Example done   Unit test
    functions.put("FORSTEP", new FORSTEP("FORSTEP"));                  // doc/einstein/function_FORSTEP          Example done   Unit test
    functions.put("FOREACH", new FOREACH("FOREACH"));                  // doc/einstein/function_FOREACH          Example done   Unit test
    functions.put("BREAK", new BREAK("BREAK"));
    functions.put("CONTINUE", new CONTINUE("CONTINUE"));
    functions.put("EVERY", new EVERY("EVERY"));
    functions.put("RANGE", new RANGE("RANGE"));
    
    //
    // Macro end
    //
    
    functions.put("RETURN", new RETURN("RETURN"));
    functions.put("NRETURN", new NRETURN("NRETURN"));
    
    //
    // GTS standalone functions
    //
    
    functions.put("NEWGTS", new NEWGTS("NEWGTS"));
    functions.put("MAKEGTS", new MAKEGTS("MAKEGTS"));
    functions.put("ADDVALUE", new ADDVALUE("ADDVALUE", false));
    functions.put("SETVALUE", new ADDVALUE("SETVALUE", true));
    functions.put("FETCH", new FETCH("FETCH", false, null));
    functions.put("FETCHLONG", new FETCH("FETCHLONG", false, TYPE.LONG));
    functions.put("FETCHDOUBLE", new FETCH("FETCHDOUBLE", false, TYPE.DOUBLE));
    functions.put("FETCHSTRING", new FETCH("FETCHSTRING", false, TYPE.STRING));
    functions.put("FETCHBOOLEAN", new FETCH("FETCHBOOLEAN", false, TYPE.BOOLEAN));
    functions.put("AFETCH", new FETCH("AFETCH", true, null));
    functions.put("LIMIT", new LIMIT("LIMIT"));
    functions.put("MAXGTS", new MAXGTS("MAXGTS"));
    functions.put("FIND", new FIND("FIND", false));
    functions.put("FINDSETS", new FIND("FINDSETS", true));
    functions.put("FINDSTATS", new FINDSTATS("FINDSTATS"));
    functions.put("DEDUP", new DEDUP("DEDUP"));
    functions.put("ONLYBUCKETS", new ONLYBUCKETS("ONLYBUCKETS"));
    functions.put("VALUEDEDUP", new VALUEDEDUP("VALUEDEDUP"));
    functions.put("CLONEEMPTY", new CLONEEMPTY("CLONEEMPTY"));
    functions.put("COMPACT", new COMPACT("COMPACT"));
    functions.put("RANGECOMPACT", new RANGECOMPACT("RANGECOMPACT"));
    functions.put("STANDARDIZE", new STANDARDIZE("STANDARDIZE"));
    functions.put("NORMALIZE", new NORMALIZE("NORMALIZE"));
    functions.put("ISONORMALIZE", new ISONORMALIZE("ISONORMALIZE"));
    functions.put("ZSCORE", new ZSCORE("ZSCORE"));
    functions.put("FILLPREVIOUS", new FILLPREVIOUS("FILLPREVIOUS"));
    functions.put("FILLNEXT", new FILLNEXT("FILLNEXT"));
    functions.put("FILLVALUE", new FILLVALUE("FILLVALUE"));
    functions.put("FILLTICKS", new FILLTICKS("FILLTICKS"));
    functions.put("INTERPOLATE", new INTERPOLATE("INTERPOLATE"));
    functions.put("FIRSTTICK", new FIRSTTICK("FIRSTTICK"));
    functions.put("LASTTICK", new LASTTICK("LASTTICK"));
    functions.put("MERGE", new MERGE("MERGE"));
    functions.put("RESETS", new RESETS("RESETS"));
    functions.put("MONOTONIC", new MONOTONIC("MONOTONIC"));
    functions.put("TIMESPLIT", new TIMESPLIT("TIMESPLIT"));
    functions.put("TIMECLIP", new TIMECLIP("TIMECLIP"));
    functions.put("TIMEMODULO", new TIMEMODULO("TIMEMODULO"));
    functions.put("CHUNK", new CHUNK("CHUNK"));
    functions.put("FUSE", new FUSE("FUSE"));
    functions.put("RENAME", new RENAME("RENAME"));
    functions.put("RELABEL", new RELABEL("RELABEL"));
    functions.put("SETATTRIBUTES", new SETATTRIBUTES("SETATTRIBUTES"));
    functions.put("CROP", new CROP("CROP"));
    functions.put("TIMESHIFT", new TIMESHIFT("TIMESHIFT"));
    functions.put("TIMESCALE", new TIMESCALE("TIMESCALE"));
    functions.put("TICKINDEX", new TICKINDEX("TICKINDEX"));
    functions.put("FFT", new FFT.Builder("FFT", true));
    functions.put("FFTAP", new FFT.Builder("FFT", false));
    functions.put("IFFT", new IFFT.Builder("IFFT"));
    functions.put("FDWT", new FDWT("FDWT"));
    functions.put("IDWT", new IDWT("IDWT"));
    functions.put("DWTSPLIT", new DWTSPLIT("DWTSPLIT"));
    functions.put("NONEMPTY", new NONEMPTY("NONEMPTY"));
    functions.put("PARTITION", new PARTITION("PARTITION"));
    functions.put("STRICTPARTITION", new PARTITION("STRICTPARTITION", true));
    functions.put("ZIP", new ZIP("ZIP"));
    functions.put("PATTERNS", new PATTERNS("PATTERNS"));
    functions.put("PATTERNDETECTION", new PATTERNDETECTION("PATTERNDETECTION"));
    functions.put("DTW", new DTW("DTW"));
    functions.put("OPTDTW", new OPTDTW("OPTDTW"));
    functions.put("VALUEHISTOGRAM", new VALUEHISTOGRAM("VALUEHISTORGRAM"));
    functions.put("PROBABILITY", new PROBABILITY.Builder("PROBABILITY"));
    functions.put("HASH", new HASH("HASH"));
    functions.put("SINGLEEXPONENTIALSMOOTHING", new SINGLEEXPONENTIALSMOOTHING("SINGLEEXPONENTIALSMOOTHING"));
    functions.put("DOUBLEEXPONENTIALSMOOTHING", new DOUBLEEXPONENTIALSMOOTHING("DOUBLEEXPONENTIALSMOOTHING"));
    functions.put("LOWESS", new LOWESS("LOWESS"));
    functions.put("RLOWESS", new RLOWESS("RLOWESS"));
    functions.put("STL", new STL("STL"));
    functions.put("LOCATIONOFFSET", new LOCATIONOFFSET("LOCATIONOFFSET"));
    functions.put("FLATTEN", new FLATTEN("FLATTEN"));
    functions.put("CORRELATE", new CORRELATE.Builder("CORRELATE"));
    functions.put("SORT", new SORT("SORT"));
    functions.put("RSORT", new RSORT("RSORT"));
    functions.put("LASTSORT", new LASTSORT("LASTSORT"));
    functions.put("METASORT", new METASORT("METASORT"));
    functions.put("VALUESORT", new VALUESORT("VALUESORT"));
    functions.put("RVALUESORT", new RVALUESORT("RVALUESORT"));
    functions.put("LSORT", new LSORT("LSORT"));
    functions.put("MSORT", new MSORT("MSORT"));
    functions.put("UPDATE", new UPDATE("UPDATE"));
    functions.put("META", new META("META"));
    functions.put("DELETE", new DELETE("DELETE"));
    functions.put("WEBCALL", new WEBCALL("WEBCALL"));
    functions.put("URLFETCH", new URLFETCH("URLFETCH"));
    functions.put("MATCH", new MATCH("MATCH"));
    functions.put("MATCHER", new MATCHER("MATCHER"));
    functions.put("TEMPLATE", new TEMPLATE("TEMPLATE"));
    functions.put("DISCORDS", new DISCORDS("DISCORDS"));
    functions.put("INTEGRATE", new INTEGRATE("INTEGRATE"));
    
    functions.put("BUCKETSPAN", new BUCKETSPAN("BUCKETSPAN"));
    functions.put("BUCKETCOUNT", new BUCKETCOUNT("BUCKETCOUNT"));
    functions.put("UNBUCKETIZE", new UNBUCKETIZE("UNBUCKETIZE"));
    functions.put("LASTBUCKET", new LASTBUCKET("LASTBUCKET"));
    functions.put("NAME", new NAME("NAME"));
    functions.put("LABELS", new LABELS("LABELS"));
    functions.put("ATTRIBUTES", new ATTRIBUTES("ATTRIBUTES"));
    functions.put("TICKS", new TICKS("TICKS"));
    functions.put("LOCATIONS", new LOCATIONS("LOCATIONS"));
    functions.put("LOCSTRINGS", new LOCSTRINGS("LOCSTRINGS"));
    functions.put("ELEVATIONS", new ELEVATIONS("ELEVATIONS"));
    functions.put("VALUES", new VALUES("VALUES"));
    functions.put("VALUESPLIT", new VALUESPLIT("VALUESPLIT"));
    functions.put("TICKLIST", new TICKLIST("TICKLIST"));
    functions.put("COMMONTICKS", new COMMONTICKS("COMMONTICKS"));
    functions.put("WRAP", new WRAP("WRAP"));
    functions.put("UNWRAP", new UNWRAP("UNWRAP"));
    
    //
    // Outlier detection
    //
    
    functions.put("THRESHOLDTEST", new THRESHOLDTEST("THRESHOLDTEST"));
    functions.put("ZSCORETEST", new ZSCORETEST("ZSCORETEST"));
    functions.put("GRUBBSTEST", new GRUBBSTEST("GRUBBSTEST"));
    functions.put("ESDTEST", new ESDTEST("ESDTEST"));
    functions.put("STLESDTEST", new STLESDTEST("STLESDTEST"));
    functions.put("HYBRIDTEST", new HYBRIDTEST("HYBRIDTEST"));
    functions.put("HYBRIDTEST2", new HYBRIDTEST2("HYBRIDTEST2"));
    
    //
    // Quaternion related functions
    //
    
    functions.put("->Q", new TOQUATERNION("->Q"));
    functions.put("Q->", new QUATERNIONTO("Q->"));
    functions.put("QCONJUGATE", new QCONJUGATE("QCONJUGATE"));
    functions.put("QDIVIDE", new QDIVIDE("QDIVIDE"));
    functions.put("QMULTIPLY", new QMULTIPLY("QMULTIPLY"));
    functions.put("QROTATE", new QROTATE("QROTATE"));
    functions.put("QROTATION", new QROTATION("QROTATION"));
    functions.put("ROTATIONQ", new ROTATIONQ("ROTATIONQ"));
    
    functions.put("ATINDEX", new ATINDEX("ATINDEX"));                   // doc/einstein/function_ATINDEX        Example done    Unit test
    functions.put("ATTICK", new ATTICK("ATTICK"));                      // doc/einstein/function_ATTICK         Example done    Unit test
    functions.put("ATBUCKET", new ATBUCKET("ATBUCKET"));                // doc/einstein/function_ATBUCKET       Example done    Unit test
    
    functions.put("CLONE", new CLONE("CLONE"));                         // doc/einstein/function_CLONE          Example done    Refactored
    functions.put("DURATION", new DURATION("DURATION"));                // doc/einstein/function_DURATION       Example done    Unit test
    functions.put("HUMANDURATION", new HUMANDURATION("HUMANDURATION"));
    functions.put("ISODURATION", new ISODURATION("ISODURATION"));
    functions.put("ISO8601", new ISO8601("ISO8601"));                   // doc/einstein/function_ISO8601        Example done    Unit test
    functions.put("TOTIMESTAMP", new TOTIMESTAMP("TOTIMESTAMP"));
    functions.put("NOTBEFORE", new NOTBEFORE("NOTBEFORE"));
    functions.put("NOTAFTER", new NOTAFTER("NOTAFTER"));
    functions.put("TSELEMENTS", new TSELEMENTS("TSELEMENTS"));
    
    functions.put("QUANTIZE", new QUANTIZE("QUANTIZE"));
    functions.put("NBOUNDS", new NBOUNDS("NBOUNDS"));
    functions.put("LBOUNDS", new LBOUNDS("LBOUNDS"));
    
    //NFIRST -> Retain at most the N first values
    //NLAST -> Retain at most the N last values
    
    //
    // GTS manipulation frameworks
    //
    
    functions.put("BUCKETIZE", new BUCKETIZE("BUCKETIZE"));
    functions.put("MAP", new MAP("MAP"));
    functions.put("FILTER", new FILTER("FILTER", true));
    functions.put("APPLY", new APPLY("APPLY", true));
    functions.put("PFILTER", new FILTER("FILTER", false));
    functions.put("PAPPLY", new APPLY("APPLY", false));
    functions.put("REDUCE", new REDUCE("REDUCE", true));
    functions.put("PREDUCE", new REDUCE("PREDUCE", false));
    
    functions.put("max.tick.sliding.window", new MaxTickSlidingWindow("max.tick.sliding.window"));
    functions.put("max.time.sliding.window", new MaxTimeSlidingWindow("max.time.sliding.window"));
    functions.put("NULL", new NULL("NULL"));
    functions.put("ISNULL", new ISNULL("ISNULL"));
    functions.put("mapper.replace", new MapperReplace.Builder("mapper.replace"));
    functions.put("mapper.gt", new MAPPERGT("mapper.gt"));
    functions.put("mapper.ge", new MAPPERGE("mapper.ge"));
    functions.put("mapper.eq", new MAPPEREQ("mapper.eq"));
    functions.put("mapper.ne", new MAPPERNE("mapper.ne"));
    functions.put("mapper.le", new MAPPERLE("mapper.le"));
    functions.put("mapper.lt", new MAPPERLT("mapper.lt"));
    functions.put("mapper.add", new MapperAdd.Builder("mapper.add"));
    functions.put("mapper.mul", new MapperMul.Builder("mapper.mul"));
    functions.put("mapper.pow", new MapperPow.Builder("mapper.pow"));
    functions.put("mapper.exp", new MapperExp.Builder("mapper.exp"));
    functions.put("mapper.log", new MapperLog.Builder("mapper.log"));
    functions.put("mapper.min.x", new MapperMinX.Builder("mapper.min.x"));          
    functions.put("mapper.max.x", new MapperMaxX.Builder("mapper.max.x"));          

    
    functions.put("mapper.tick", new MapperTick.Builder("mapper.tick"));
    functions.put("mapper.year", new MapperYear.Builder("mapper.year"));
    functions.put("mapper.month", new MapperMonthOfYear.Builder("mapper.month"));
    functions.put("mapper.day", new MapperDayOfMonth.Builder("mapper.day"));
    functions.put("mapper.weekday", new MapperDayOfWeek.Builder("mapper.weekday"));
    functions.put("mapper.hour", new MapperHourOfDay.Builder("mapper.hour"));
    functions.put("mapper.minute", new MapperMinuteOfHour.Builder("mapper.minute"));
    functions.put("mapper.second", new MapperSecondOfMinute.Builder("mapper.second"));
    
    functions.put("mapper.npdf", new MapperNPDF.Builder("mapper.npdf"));
    functions.put("mapper.dotproduct", new MapperDotProduct.Builder("mapper.dotproduct"));
    functions.put("mapper.dotproduct.tanh", new MapperDotProductTanh.Builder("mapper.dotproduct.tanh"));
    functions.put("mapper.dotproduct.sigmoid", new MapperDotProductSigmoid.Builder("mapper.dotproduct.sigmoid"));
    functions.put("mapper.dotproduct.positive", new MapperDotProductPositive.Builder("mapper.dotproduct.positive"));

    // Kernel mappers
    functions.put("mapper.kernel.cosine", new MapperKernelCosine("mapper.kernel.cosine"));
    functions.put("mapper.kernel.epanechnikov", new MapperKernelEpanechnikov("mapper.kernel.epanechnikov"));
    functions.put("mapper.kernel.gaussian", new MapperKernelGaussian("mapper.kernel.gaussian"));
    functions.put("mapper.kernel.logistic", new MapperKernelLogistic("mapper.kernel.logistic"));
    functions.put("mapper.kernel.quartic", new MapperKernelQuartic("mapper.kernel.quartic"));
    functions.put("mapper.kernel.silverman", new MapperKernelSilverman("mapper.kernel.silverman"));
    functions.put("mapper.kernel.triangular", new MapperKernelTriangular("mapper.kernel.triangular"));
    functions.put("mapper.kernel.tricube", new MapperKernelTricube("mapper.kernel.tricube"));
    functions.put("mapper.kernel.triweight", new MapperKernelTriweight("mapper.kernel.triweight"));
    functions.put("mapper.kernel.uniform", new MapperKernelUniform("mapper.kernel.uniform"));
        
    functions.put("mapper.percentile", new Percentile.Builder("mapper.percentile"));
    
    //functions.put("mapper.abscissa", new MapperSAX.Builder());
    
    functions.put("filter.byclass", new FilterByClass.Builder("filter.byclass"));
    functions.put("filter.bylabels", new FilterByLabels.Builder("filter.bylabels"));
    functions.put("filter.bymetadata", new FilterByMetadata.Builder("filter.bymetadata"));
    functions.put("filter.last.eq", new FilterLastEQ.Builder("filter.last.eq"));
    functions.put("filter.last.ge", new FilterLastGE.Builder("filter.last.ge"));
    functions.put("filter.last.gt", new FilterLastGT.Builder("filter.last.gt"));
    functions.put("filter.last.le", new FilterLastLE.Builder("filter.last.le"));
    functions.put("filter.last.lt", new FilterLastLT.Builder("filter.last.lt"));
    functions.put("filter.last.ne", new FilterLastNE.Builder("filter.last.ne"));
    
    functions.put("filter.latencies", new LatencyFilter.Builder("filter.latencies"));
    
    //
    // Geo Manipulation functions
    //
    
    functions.put(GEO_WKT, new GeoWKT(GEO_WKT));
    functions.put(GEO_INTERSECTION, new GeoIntersection(GEO_INTERSECTION));
    functions.put(GEO_UNION, new GeoUnion(GEO_UNION));
    functions.put(GEO_DIFFERENCE, new GeoSubtraction(GEO_DIFFERENCE));
    functions.put("GEO.WITHIN", new GEOWITHIN("GEO.WITHIN"));
    functions.put("GEO.INTERSECTS", new GEOINTERSECTS("GEO.WINTERSECTS"));
    functions.put("HAVERSINE", new HAVERSINE("HAVERSINE"));
    functions.put("mapper.geo.within", new MapperGeoWithin.Builder("mapper.geo.within"));
    functions.put("mapper.geo.outside", new MapperGeoOutside.Builder("mapper.geo.outside"));
    functions.put("mapper.geo.approximate", new MapperGeoApproximate.Builder("mapper.geo.approximate"));
    functions.put("COPYGEO", new COPYGEO("COPYGEO"));
    functions.put("BBOX", new BBOX("BBOX"));
    
    //
    // Counters
    //
    
    functions.put("COUNTER", new COUNTER("COUNTER"));
    functions.put("COUNTERVALUE", new COUNTERVALUE("COUNTERVALUE"));
    functions.put("COUNTERDELTA", new COUNTERDELTA("COUNTERDELTA"));
    
    //
    // LoRaWAN
    //
    
    functions.put("LORAMIC", new LORAMIC("LORAMIC"));
    functions.put("LORAENC", new LORAENC("LORAENC"));
    
    //
    // Math functions
    //
    
    functions.put("pi", new Pi("pi"));
    functions.put("PI", new Pi("PI"));
    functions.put("e", new E("e"));
    functions.put("E", new E("E"));
    functions.put("MINLONG", new MINLONG("MINLONG"));
    functions.put("MAXLONG", new MAXLONG("MAXLONG"));
    functions.put("RAND", new RAND("RAND"));

    functions.put("NPDF", new NPDF.Builder("NPDF"));
    functions.put("MUSIGMA", new MUSIGMA("MUSIGMA"));
    functions.put("NSUMSUMSQ", new NSUMSUMSQ("NSUMSUMSQ"));
    
    try {

      functions.put("COS", new MATH("COS", "cos"));
      functions.put("COSH", new MATH("COSH", "cosh"));
      functions.put("ACOS", new MATH("ACOS", "acos"));
      
      functions.put("SIN", new MATH("SIN", "sin"));
      functions.put("SINH", new MATH("SINH", "sinh"));
      functions.put("ASIN", new MATH("ASIN", "asin"));

      functions.put("TAN", new MATH("TAN", "tan"));
      functions.put("TANH", new MATH("TANH", "tanh"));
      functions.put("ATAN", new MATH("ATAN", "atan"));


      functions.put("SIGNUM", new MATH("SIGNUM", "signum"));
      functions.put("FLOOR", new MATH("FLOOR", "floor"));
      functions.put("CEIL", new MATH("CEIL", "ceil"));
      functions.put("ROUND", new MATH("ROUND", "round"));

      functions.put("RINT", new MATH("RINT", "rint"));
      functions.put("NEXTUP", new MATH("NEXTUP", "nextUp"));
      functions.put("ULP", new MATH("ULP", "ulp"));

      functions.put("SQRT", new MATH("SQRT", "sqrt"));
      functions.put("CBRT", new MATH("CBRT", "cbrt"));
      functions.put("EXP", new MATH("EXP", "exp"));
      functions.put("EXPM1", new MATH("EXPM1", "expm1"));
      functions.put("LOG", new MATH("LOG", "log"));
      functions.put("LOG10", new MATH("LOG10", "log10"));
      functions.put("LOG1P", new MATH("LOG1P", "log1p"));

      functions.put("TORADIANS", new MATH("TORADIANS", "toRadians"));
      functions.put("TODEGREES", new MATH("TODEGREES", "toDegrees"));

      functions.put("MAX", new MATH2("MAX", "max"));
      functions.put("MIN", new MATH2("MIN", "min"));

      functions.put("COPYSIGN", new MATH2("COPYSIGN", "copySign"));
      functions.put("HYPOT", new MATH2("HYPOT", "hypot"));
      functions.put("IEEEREMAINDER", new MATH2("IEEEREMAINDER", "IEEEremainder"));
      functions.put("NEXTAFTER", new MATH2("NEXTAFTER", "nextAfter"));

    } catch (WarpScriptException ee) {
      throw new RuntimeException(ee);
    }
    
    functions.put("IDENT", new IDENT("IDENT"));
    
    //
    // Processing
    //
    
    functions.put("Pencode", new Pencode("Pencode"));
    
    // Structure
    
    functions.put("PpushStyle", new PpushStyle("PpushStyle"));
    functions.put("PpopStyle", new PpopStyle("PpopStyle"));

    // Environment
    
    
    // Shape
    
    functions.put("Parc", new Parc("Parc"));
    functions.put("Pellipse", new Pellipse("Pellipse"));
    functions.put("Ppoint", new Ppoint("Ppoint"));
    functions.put("Pline", new Pline("Pline"));
    functions.put("Ptriangle", new Ptriangle("Ptriangle"));
    functions.put("Prect", new Prect("Prect"));
    functions.put("Pquad", new Pquad("Pquad"));
    
    functions.put("Pbezier", new Pbezier("Pbezier"));
    functions.put("PbezierPoint", new PbezierPoint("PbezierPoint"));
    functions.put("PbezierTangent", new PbezierTangent("PbezierTangent"));
    functions.put("PbezierDetail", new PbezierDetail("PbezierDetail"));
    
    functions.put("Pcurve", new Pcurve("Pcurve"));
    functions.put("PcurvePoint", new PcurvePoint("PcurvePoint"));
    functions.put("PcurveTangent", new PcurveTangent("PcurveTangent"));
    functions.put("PcurveDetail", new PcurveDetail("PcurveDetail"));
    functions.put("PcurveTightness", new PcurveTightness("PcurveTightness"));

    functions.put("Pbox", new Pbox("Pbox"));
    functions.put("Psphere", new Psphere("Psphere"));
    functions.put("PsphereDetail", new PsphereDetail("PsphereDetail"));
    
    functions.put("PellipseMode", new PellipseMode("PellipseMode"));
    functions.put("PrectMode", new PrectMode("PrectMode"));
    functions.put("PstrokeCap", new PstrokeCap("PstrokeCap"));
    functions.put("PstrokeJoin", new PstrokeJoin("PstrokeJoin"));
    functions.put("PstrokeWeight", new PstrokeWeight("PstrokeWeight"));
    
    functions.put("PbeginShape", new PbeginShape("PbeginShape"));
    functions.put("PendShape", new PendShape("PendShape"));
    functions.put("PbeginContour", new PbeginContour("PbeginContour"));
    functions.put("PendContour", new PendContour("PendContour"));
    functions.put("Pvertex", new Pvertex("Pvertex"));
    functions.put("PcurveVertex", new PcurveVertex("PcurveVertex"));
    functions.put("PbezierVertex", new PbezierVertex("PbezierVertex"));
    functions.put("PquadraticVertex", new PquadraticVertex("PquadraticVertex"));
    
    // TODO(hbs): support PShape (need to support PbeginShape etc applied to PShape instances)
    //functions.put("PshapeMode", new PshapeMode("PshapeMode"));
    
    // Transform
    
    functions.put("PpushMatrix", new PpushMatrix("PpushMatrix"));
    functions.put("PpopMatrix", new PpopMatrix("PpopMatrix"));
    functions.put("PresetMatrix", new PresetMatrix("PresetMatrix"));
    functions.put("Protate", new Protate("Protate"));
    functions.put("ProtateX", new ProtateX("ProtateX"));
    functions.put("ProtateY", new ProtateY("ProtateY"));
    functions.put("ProtateZ", new ProtateZ("ProtateZ"));
    functions.put("Pscale", new Pscale("Pscale"));
    functions.put("PshearX", new PshearX("PshearX"));
    functions.put("PshearY", new PshearY("PshearY"));
    functions.put("Ptranslate", new Ptranslate("Ptranslate"));
    
    // Color
    
    functions.put("Pbackground", new Pbackground("Pbackground"));
    functions.put("PcolorMode", new PcolorMode("PcolorMode"));
    functions.put("Pclear", new Pclear("Pclear"));
    functions.put("Pfill", new Pfill("Pfill"));
    functions.put("PnoFill", new PnoFill("PnoFill"));
    functions.put("Pstroke", new Pstroke("Pstroke"));
    functions.put("PnoStroke", new PnoStroke("PnoStroke"));
    
    functions.put("Palpha", new Palpha("Palpha"));
    functions.put("Pblue", new Pblue("Pblue"));
    functions.put("Pbrightness", new Pbrightness("Pbrightness"));
    functions.put("Pcolor", new Pcolor("Pcolor"));
    functions.put("Pgreen", new Pgreen("Pgreen"));
    functions.put("Phue", new Phue("Phue"));
    functions.put("PlerpColor", new PlerpColor("PlerpColor"));
    functions.put("Pred", new Pred("Pred"));
    functions.put("Psaturation", new Psaturation("Psaturation"));
    
    // Image
    
    functions.put("Pdecode", new Pdecode("Pdecode"));
    functions.put("Pimage", new Pimage("Pimage"));
    functions.put("PimageMode", new PimageMode("PimageMode"));
    functions.put("Ptint", new Ptint("Ptint"));
    functions.put("PnoTint", new PnoTint("PnoTint"));
    functions.put("Ppixels", new Ppixels("Ppixels"));
    functions.put("PupdatePixels", new PupdatePixels("PupdatePixels"));
    
    // TODO(hbs): support texture related functions?
    
    functions.put("Pblend", new Pblend("Pblend"));
    functions.put("Pcopy", new Pcopy("Pcopy"));
    functions.put("Pget", new Pget("Pget"));
    functions.put("Pset", new Pset("Pset"));
    
    // Rendering
    
    functions.put("PblendMode", new PblendMode("PblendMode"));
    functions.put("Pclip", new Pclip("Pclip"));
    functions.put("PnoClip", new PnoClip("PnoClip"));
    functions.put("PGraphics", new PGraphics("PGraphics"));

    // TODO(hbs): support shaders?
    
    // Typography
    
    functions.put("PcreateFont", new PcreateFont("PcreateFont"));
    functions.put("Ptext", new Ptext("Ptext"));
    functions.put("PtextAlign", new PtextAlign("PtextAlign"));
    functions.put("PtextAscent", new PtextAscent("PtextAscent"));
    functions.put("PtextDescent", new PtextDescent("PtextDescent"));
    functions.put("PtextFont", new PtextFont("PtextFont"));
    functions.put("PtextLeading", new PtextLeading("PtextLeading"));
    functions.put("PtextMode", new PtextMode("PtextMode"));
    functions.put("PtextSize", new PtextSize("PtextSize"));
    functions.put("PtextWidth", new PtextWidth("PtextWidth"));
    
    // Math
    
    functions.put("Pconstrain", new Pconstrain("Pconstrain"));
    functions.put("Pdist", new Pdist("Pdist"));
    functions.put("Plerp", new Plerp("Plert"));
    functions.put("Pmag", new Pmag("Pmag"));
    functions.put("Pmap", new Pmap("Pmap"));
    functions.put("Pnorm", new Pnorm("Pnorm"));
    
    ////////////////////////////////////////////////////////////////////////////
    
    //
    // TBD
    //
    
    functions.put("mapper.distinct", new FAIL("mapper.distinct")); // Counts the number of distinct values in a window, using HyperLogLog???
    functions.put("TICKSHIFT", new FAIL("TICKSHIFT")); // Shifts the ticks of a GTS by this many positions
    
    Properties props = WarpConfig.getProperties();
      
    if (null != props && props.containsKey(Configuration.CONFIG_WARPSCRIPT_LANGUAGES)) {
      String[] languages = props.getProperty(Configuration.CONFIG_WARPSCRIPT_LANGUAGES).split(",");
      Set<String> lang = new HashSet<String>();
      
      for (String language: languages) {
        lang.add(language.toUpperCase());
      }
      
      if (lang.contains("R")) {
        functions.put("R", new R("R"));
        functions.put("R->", new RTO("R->"));        
      }      
      if (lang.contains("JS")) {
        functions.put("JS", new JS("JS"));        
      }      
      if (lang.contains("GROOVY")) {
        functions.put("GROOVY", new SCRIPTENGINE("GROOVY", "groovy"));        
      }      
      if (lang.contains("LUA")) {
        functions.put("LUA", new LUA("LUA"));        
      }      
      if (lang.contains("RUBY")) {
        functions.put("RUBY", new RUBY("RUBY"));        
      }
      if (lang.contains("PYTHON")) {
        functions.put("PYTHON", new PYTHON("PYTHON"));              
      }
    }    
  }
  
  public static WarpScriptStackFunction getFunction(String name) {
    return functions.get(name);
  }
  
  /**
   * Check whether or not an object is castable to a macro
   * @param o
   * @return
   */
  public static boolean isMacro(Object o) {
    
    if (null == o) {
      return false;
    }
    
    return o instanceof Macro;
  }
}
