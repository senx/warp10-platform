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

import io.warp10.WarpClassLoader;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.CORRELATE;
import io.warp10.continuum.gts.DISCORDS;
import io.warp10.continuum.gts.FFT;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.gts.IFFT;
import io.warp10.continuum.gts.INTERPOLATE;
import io.warp10.continuum.gts.LOCATIONOFFSET;
import io.warp10.continuum.gts.ZIP;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.aggregator.And;
import io.warp10.script.aggregator.Argmax;
import io.warp10.script.aggregator.Argmin;
import io.warp10.script.aggregator.CircularMean;
import io.warp10.script.aggregator.Count;
import io.warp10.script.aggregator.Delta;
import io.warp10.script.aggregator.First;
import io.warp10.script.aggregator.HDist;
import io.warp10.script.aggregator.HSpeed;
import io.warp10.script.aggregator.Highest;
import io.warp10.script.aggregator.Join;
import io.warp10.script.aggregator.Last;
import io.warp10.script.aggregator.Lowest;
import io.warp10.script.aggregator.MAD;
import io.warp10.script.aggregator.Max;
import io.warp10.script.aggregator.Mean;
import io.warp10.script.aggregator.Median;
import io.warp10.script.aggregator.Min;
import io.warp10.script.aggregator.Or;
import io.warp10.script.aggregator.Percentile;
import io.warp10.script.aggregator.Rate;
import io.warp10.script.aggregator.ShannonEntropy;
import io.warp10.script.aggregator.StandardDeviation;
import io.warp10.script.aggregator.Sum;
import io.warp10.script.aggregator.TrueCourse;
import io.warp10.script.aggregator.VDist;
import io.warp10.script.aggregator.VSpeed;
import io.warp10.script.aggregator.Variance;
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
import io.warp10.script.binary.INPLACEADD;
import io.warp10.script.binary.LE;
import io.warp10.script.binary.LT;
import io.warp10.script.binary.MOD;
import io.warp10.script.binary.MUL;
import io.warp10.script.binary.NE;
import io.warp10.script.binary.POW;
import io.warp10.script.binary.SHIFTLEFT;
import io.warp10.script.binary.SHIFTRIGHT;
import io.warp10.script.binary.SUB;
import io.warp10.script.ext.groovy.GroovyWarpScriptExtension;
import io.warp10.script.ext.js.JSWarpScriptExtension;
import io.warp10.script.ext.lua.LUAWarpScriptExtension;
import io.warp10.script.ext.python.PythonWarpScriptExtension;
import io.warp10.script.ext.r.RWarpScriptExtension;
import io.warp10.script.ext.ruby.RubyWarpScriptExtension;
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
import io.warp10.script.mapper.MapperAbs;
import io.warp10.script.mapper.MapperAdd;
import io.warp10.script.mapper.MapperCeil;
import io.warp10.script.mapper.MapperDayOfMonth;
import io.warp10.script.mapper.MapperDayOfWeek;
import io.warp10.script.mapper.MapperDotProduct;
import io.warp10.script.mapper.MapperDotProductPositive;
import io.warp10.script.mapper.MapperDotProductSigmoid;
import io.warp10.script.mapper.MapperDotProductTanh;
import io.warp10.script.mapper.MapperExp;
import io.warp10.script.mapper.MapperFinite;
import io.warp10.script.mapper.MapperFloor;
import io.warp10.script.mapper.MapperGeoApproximate;
import io.warp10.script.mapper.MapperGeoClearPosition;
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
import io.warp10.script.mapper.MapperMod;
import io.warp10.script.mapper.MapperMonthOfYear;
import io.warp10.script.mapper.MapperMul;
import io.warp10.script.mapper.MapperNPDF;
import io.warp10.script.mapper.MapperParseDouble;
import io.warp10.script.mapper.MapperPow;
import io.warp10.script.mapper.MapperProduct;
import io.warp10.script.mapper.MapperReplace;
import io.warp10.script.mapper.MapperRound;
import io.warp10.script.mapper.MapperSecondOfMinute;
import io.warp10.script.mapper.MapperSigmoid;
import io.warp10.script.mapper.MapperTanh;
import io.warp10.script.mapper.MapperTick;
import io.warp10.script.mapper.MapperToBoolean;
import io.warp10.script.mapper.MapperToDouble;
import io.warp10.script.mapper.MapperToLong;
import io.warp10.script.mapper.MapperToString;
import io.warp10.script.mapper.MapperYear;
import io.warp10.script.mapper.STRICTMAPPER;
import io.warp10.script.op.OpAND;
import io.warp10.script.op.OpAdd;
import io.warp10.script.op.OpDiv;
import io.warp10.script.op.OpEQ;
import io.warp10.script.op.OpGE;
import io.warp10.script.op.OpGT;
import io.warp10.script.op.OpLE;
import io.warp10.script.op.OpLT;
import io.warp10.script.op.OpMask;
import io.warp10.script.op.OpMul;
import io.warp10.script.op.OpNE;
import io.warp10.script.op.OpOR;
import io.warp10.script.op.OpSub;
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
import io.warp10.warp.sdk.WarpScriptExtension;

import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Library of functions used to manipulate Geo Time Series
 * and more generally interact with a WarpScriptStack
 */
public class WarpScriptLib {
  
  private static final Logger LOG = LoggerFactory.getLogger(WarpScriptLib.class);
  
  private static Map<String,Object> functions = new HashMap<String, Object>();
  
  /**
   * Static definition of name so it can be reused outside of WarpScriptLib
   */
  
  public static final String NULL = "NULL";
  
  public static final String EVAL = "EVAL";
  public static final String EVALSECURE = "EVALSECURE";
  public static final String SNAPSHOT = "SNAPSHOT";
  public static final String SNAPSHOTALL = "SNAPSHOTALL";
  public static final String LOAD = "LOAD";
  public static final String RUN = "RUN";
  public static final String BOOTSTRAP = "BOOTSTRAP";
  
  public static final String MAP_START = "{";
  public static final String MAP_END = "}";

  public static final String LIST_START = "[";
  public static final String LIST_END = "]";

  public static final String TO_VECTOR = "->V";
  public static final String TO_SET = "->SET";
  
  public static final String NEWGTS = "NEWGTS";
  public static final String SWAP = "SWAP";
  public static final String RELABEL = "RELABEL";
  public static final String RENAME = "RENAME";
  public static final String PARSESELECTOR = "PARSESELECTOR";
  
  public static final String GEO_WKT = "GEO.WKT";
  public static final String GEO_INTERSECTION = "GEO.INTERSECTION";
  public static final String GEO_DIFFERENCE = "GEO.DIFFERENCE";
  public static final String GEO_UNION = "GEO.UNION";
  public static final String GEOPACK = "GEOPACK";
  public static final String GEOUNPACK = "GEOUNPACK";
  
  public static final String UNWRAP = "UNWRAP";
  public static final String OPB64TO = "OPB64->";
  public static final String TOOPB64 = "->OPB64";
  public static final String BYTESTO = "BYTES->";
  public static final String BYTESTOBITS = "BYTESTOBITS";
  public static final String MARK = "MARK";
  public static final String STORE = "STORE";
  
  public static final String MAPPER_HIGHEST = "mapper.highest";
  public static final String MAPPER_LOWEST = "mapper.lowest";
  public static final String MAPPER_MAX = "mapper.max";
  public static final String MAPPER_MIN = "mapper.min";
  
  public static final String RSAPUBLIC = "RSAPUBLIC";
  public static final String RSAPRIVATE = "RSAPRIVATE";
  
  static {
    
    functions.put("REV", new REV("REV"));
    
    functions.put(BOOTSTRAP, new NOOP(BOOTSTRAP));
    
    functions.put("RTFM", new RTFM("RTFM"));
    
    functions.put("REXEC", new REXEC("REXEC"));
    functions.put("REXECZ", new REXEC("REXECZ", true));
    
    //
    // Stack manipulation functions
    //
    
    functions.put("PIGSCHEMA", new PIGSCHEMA("PIGSCHEMA"));
    functions.put(MARK, new MARK(MARK));
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
    functions.put("UNLIST", new UNLIST("UNLIST"));
    functions.put(TO_SET, new TOSET(TO_SET));
    functions.put("SET->", new SETTO("SET->"));
    functions.put(TO_VECTOR, new TOVECTOR(TO_VECTOR));
    functions.put("V->", new VECTORTO("V->"));
    functions.put("UNION", new UNION("UNION"));
    functions.put("INTERSECTION", new INTERSECTION("INTERSECTION"));
    functions.put("DIFFERENCE", new DIFFERENCE("DIFFERENCE"));
    functions.put("->MAP", new TOMAP("->MAP"));
    functions.put("MAP->", new MAPTO("MAP->"));
    functions.put("UNMAP", new UNMAP("UNMAP"));
    functions.put("MAPID", new MAPID("MAPID"));
    functions.put("->JSON", new TOJSON("->JSON"));      
    functions.put("JSON->", new JSONTO("JSON->"));
    functions.put("->PICKLE", new TOPICKLE("->PICKLE"));
    functions.put("PICKLE->", new PICKLETO("PICKLE->"));
    functions.put("GET", new GET("GET"));
    functions.put("SET", new SET("SET"));
    functions.put("PUT", new PUT("PUT"));
    functions.put("SUBMAP", new SUBMAP("SUBMAP"));
    functions.put("SUBLIST", new SUBLIST("SUBLIST"));
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
    functions.put(SWAP, new SWAP(SWAP));
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
    functions.put("AGO", new AGO("AGO"));
    functions.put("MSTU", new MSTU("MSTU"));
    functions.put("STU", new STU("STU"));
    functions.put("APPEND", new APPEND("APPEND"));
    functions.put(STORE, new STORE(STORE));
    functions.put("CSTORE", new CSTORE("CSTORE"));
    functions.put(LOAD, new LOAD(LOAD));
    functions.put(RUN, new RUN(RUN));
    functions.put("DEF", new DEF("DEF"));
    functions.put("UDF", new UDF("UDF", false));
    functions.put("CUDF", new UDF("CUDF", true));
    functions.put("CALL", new CALL("CALL"));
    functions.put("FORGET", new FORGET("FORGET"));    
    functions.put("DEFINED", new DEFINED("DEFINED"));
    functions.put("REDEFS", new REDEFS("REDEFS"));
    functions.put("DEFINEDMACRO", new DEFINEDMACRO("DEFINEDMACRO"));
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
    functions.put("NONNULL", new NONNULL("NONNULL"));
    functions.put("LFLATMAP", new LFLATMAP("LFLATMAP"));
    functions.put("[]", new EMPTYLIST("[]"));
    functions.put(LIST_START, new MARK(LIST_START));
    functions.put(LIST_END, new ENDLIST(LIST_END));
    functions.put("STACKTOLIST", new STACKTOLIST("STACKTOLIST"));
    functions.put("{}", new EMPTYMAP("{}"));
    functions.put("IMMUTABLE", new IMMUTABLE("IMMUTABLE"));
    functions.put(MAP_START, new MARK(MAP_START));
    functions.put(MAP_END, new ENDMAP(MAP_END));
    functions.put("SECUREKEY", new SECUREKEY("SECUREKEY"));
    functions.put("UNSECURE", new UNSECURE("UNSECURE", true));
    functions.put(EVALSECURE, new EVALSECURE(EVALSECURE));
    functions.put("NOOP", new NOOP("NOOP"));
    functions.put("DOC", new DOC("DOC"));
    functions.put("DOCMODE", new DOCMODE("DOCMODE"));
    functions.put("SECTION", new SECTION("SECTION"));
    functions.put("GETSECTION", new GETSECTION("GETSECTION"));
    functions.put(SNAPSHOT, new SNAPSHOT(SNAPSHOT, false, false, true));
    functions.put(SNAPSHOTALL, new SNAPSHOT(SNAPSHOTALL, true, false, true));
    functions.put("SNAPSHOTTOMARK", new SNAPSHOT("SNAPSHOTTOMARK", false, true, true));
    functions.put("SNAPSHOTALLTOMARK", new SNAPSHOT("SNAPSHOTALLTOMARK", true, true, true));
    functions.put("SNAPSHOTCOPY", new SNAPSHOT("SNAPSHOTCOPY", false, false, false));
    functions.put("SNAPSHOTCOPYALL", new SNAPSHOT("SNAPSHOTCOPYALL", true, false, false));
    functions.put("SNAPSHOTCOPYTOMARK", new SNAPSHOT("SNAPSHOTCOPYTOMARK", false, true, false));
    functions.put("SNAPSHOTCOPYALLTOMARK", new SNAPSHOT("SNAPSHOTCOPYALLTOMARK", true, true, false));
    functions.put("HEADER", new HEADER("HEADER"));
    
    functions.put("MACROMAPPER", new MACROMAPPER("MACROMAPPER"));
    functions.put("MACROREDUCER", new MACROMAPPER("MACROREDUCER"));
    functions.put("MACROBUCKETIZER", new MACROMAPPER("MACROBUCKETIZER"));
    functions.put("MACROFILTER", new MACROFILTER("MACROFILTER"));
    functions.put("STRICTMAPPER", new STRICTMAPPER("STRICTMAPPER"));
    functions.put("STRICTREDUCER", new STRICTREDUCER("STRICTREDUCER"));
    
    functions.put(PARSESELECTOR, new PARSESELECTOR(PARSESELECTOR));
    functions.put("TOSELECTOR", new TOSELECTOR("TOSELECTOR"));
    functions.put("PARSE", new PARSE("PARSE"));
        
    // We do not expose DUMP, it might allocate too much memory
    //functions.put("DUMP", new DUMP("DUMP"));
    
    // Binary ops
    functions.put("+", new ADD("+"));
    functions.put("+!", new INPLACEADD("+!"));
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

    // Bitset ops
    functions.put("BITGET", new BITGET("BITGET"));
    functions.put("BITCOUNT", new BITCOUNT("BITCOUNT"));
    functions.put("BITSTOBYTES", new BITSTOBYTES("BITSTOBYTES"));
    functions.put("BYTESTOBITS", new BYTESTOBITS("BYTESTOBITS"));
    
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
    functions.put("TOBITS", new TOBITS("TOBITS", false));
    functions.put("FROMBITS", new FROMBITS("FROMBITS", false));
    functions.put("->DOUBLEBITS", new TOBITS("->DOUBLEBITS", false));
    functions.put("DOUBLEBITS->", new FROMBITS("DOUBLEBITS->", false));
    functions.put("->FLOATBITS", new TOBITS("->FLOATBITS", true));
    functions.put("FLOATBITS->", new FROMBITS("FLOATBITS->", true));
    functions.put("TOKENINFO", new TOKENINFO("TOKENINFO"));
    functions.put("GETHOOK", new GETHOOK("GETHOOK"));
    
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
    
    // Crypto functions
    functions.put("HASH", new HASH("HASH"));
    functions.put("MD5", new DIGEST("MD5", MD5Digest.class));
    functions.put("SHA1", new DIGEST("SHA1", SHA1Digest.class));
    functions.put("SHA256", new DIGEST("SHA256", SHA256Digest.class));
    functions.put("SHA256HMAC", new HMAC("SHA256HMAC", SHA256Digest.class));
    functions.put("SHA1HMAC", new HMAC("SHA1HMAC", SHA1Digest.class));
    functions.put("AESWRAP", new AESWRAP("AESWRAP"));
    functions.put("AESUNWRAP", new AESUNWRAP("AESUNWRAP"));
    functions.put("RUNNERNONCE", new RUNNERNONCE("RUNNERNONCE"));
    functions.put("GZIP", new GZIP("GZIP"));
    functions.put("UNGZIP", new UNGZIP("UNGZIP"));
    functions.put("RSAGEN", new RSAGEN("RSAGEN"));
    functions.put(RSAPUBLIC, new RSAPUBLIC(RSAPUBLIC));
    functions.put(RSAPRIVATE, new RSAPRIVATE(RSAPRIVATE));
    functions.put("RSAENCRYPT", new RSAENCRYPT("RSAENCRYPT"));
    functions.put("RSADECRYPT", new RSADECRYPT("RSADECRYPT"));
    functions.put("RSASIGN", new RSASIGN("RSASIGN"));
    functions.put("RSAVERIFY", new RSAVERIFY("RSAVERIFY"));

    //
    // String functions
    //
    
    functions.put("URLDECODE", new URLDECODE("URLDECODE"));
    functions.put("URLENCODE", new URLENCODE("URLENCODE"));
    functions.put("SPLIT", new SPLIT("SPLIT"));
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
    functions.put(BYTESTO, new BYTESTO(BYTESTO));

    functions.put("->BYTES", new TOBYTES("->BYTES"));
    functions.put("->BIN", new io.warp10.script.functions.TOBIN("->BIN"));
    functions.put("->HEX", new io.warp10.script.functions.TOHEX("->HEX"));
    functions.put("->B64", new TOB64("->B64"));
    functions.put("->B64URL", new TOB64URL("->B64URL"));
    functions.put(TOOPB64, new TOOPB64(TOOPB64));
    functions.put(OPB64TO, new OPB64TO(OPB64TO));
    functions.put("OPB64TOHEX", new OPB64TOHEX("OPB64TOHEX"));    
    
    //
    // Conditionals
    //
    
    functions.put("IFT", new IFT("IFT"));
    functions.put("IFTE", new IFTE("IFTE"));
    functions.put("SWITCH", new SWITCH("SWITCH"));
    
    //
    // Loops
    //
    
    functions.put("WHILE", new WHILE("WHILE"));
    functions.put("UNTIL", new UNTIL("UNTIL"));
    functions.put("FOR", new FOR("FOR"));
    functions.put("FORSTEP", new FORSTEP("FORSTEP"));
    functions.put("FOREACH", new FOREACH("FOREACH"));
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
    
    functions.put(NEWGTS, new NEWGTS(NEWGTS));
    functions.put("MAKEGTS", new MAKEGTS("MAKEGTS"));
    functions.put("ADDVALUE", new ADDVALUE("ADDVALUE", false));
    functions.put("SETVALUE", new ADDVALUE("SETVALUE", true));
    functions.put("FETCH", new FETCH("FETCH", false, null));
    functions.put("FETCHLONG", new FETCH("FETCHLONG", false, TYPE.LONG));
    functions.put("FETCHDOUBLE", new FETCH("FETCHDOUBLE", false, TYPE.DOUBLE));
    functions.put("FETCHSTRING", new FETCH("FETCHSTRING", false, TYPE.STRING));
    functions.put("FETCHBOOLEAN", new FETCH("FETCHBOOLEAN", false, TYPE.BOOLEAN));
    functions.put("LIMIT", new LIMIT("LIMIT"));
    functions.put("MAXGTS", new MAXGTS("MAXGTS"));
    functions.put("FIND", new FIND("FIND", false));
    functions.put("FINDSETS", new FIND("FINDSETS", true));
    functions.put("METASET", new FIND("METASET", false, true));
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
    functions.put("CLIP", new CLIP("CLIP"));
    functions.put("TIMEMODULO", new TIMEMODULO("TIMEMODULO"));
    functions.put("CHUNK", new CHUNK("CHUNK", true));
    functions.put("FUSE", new FUSE("FUSE"));
    functions.put(RENAME, new RENAME(RENAME));
    functions.put(RELABEL, new RELABEL(RELABEL));
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
    functions.put("EMPTY", new EMPTY("EMPTY"));
    functions.put("NONEMPTY", new NONEMPTY("NONEMPTY"));
    functions.put("PARTITION", new PARTITION("PARTITION"));
    functions.put("STRICTPARTITION", new PARTITION("STRICTPARTITION", true));
    functions.put("ZIP", new ZIP("ZIP"));
    functions.put("PATTERNS", new PATTERNS("PATTERNS", true));
    functions.put("PATTERNDETECTION", new PATTERNDETECTION("PATTERNDETECTION", true));
    functions.put("ZPATTERNS", new PATTERNS("ZPATTERNS", false));
    functions.put("ZPATTERNDETECTION", new PATTERNDETECTION("ZPATTERNDETECTION", false));
    functions.put("DTW", new DTW("DTW"));
    functions.put("OPTDTW", new OPTDTW("OPTDTW"));
    functions.put("VALUEHISTOGRAM", new VALUEHISTOGRAM("VALUEHISTORGRAM"));
    functions.put("PROBABILITY", new PROBABILITY.Builder("PROBABILITY"));
    functions.put("PROB", new PROB("PROB"));
    functions.put("CPROB", new CPROB("CPROB"));
    functions.put("RANDPDF", new RANDPDF.Builder("RANDPDF"));
    functions.put("SINGLEEXPONENTIALSMOOTHING", new SINGLEEXPONENTIALSMOOTHING("SINGLEEXPONENTIALSMOOTHING"));
    functions.put("DOUBLEEXPONENTIALSMOOTHING", new DOUBLEEXPONENTIALSMOOTHING("DOUBLEEXPONENTIALSMOOTHING"));
    functions.put("LOWESS", new LOWESS("LOWESS"));
    functions.put("RLOWESS", new RLOWESS("RLOWESS"));
    functions.put("STL", new STL("STL"));
    functions.put("LTTB", new LTTB("LTTB", false));
    functions.put("TLTTB", new LTTB("TLTTB", true));
    functions.put("LOCATIONOFFSET", new LOCATIONOFFSET("LOCATIONOFFSET"));
    functions.put("FLATTEN", new FLATTEN("FLATTEN"));
    functions.put("CORRELATE", new CORRELATE.Builder("CORRELATE"));
    functions.put("SORT", new SORT("SORT"));
    functions.put("SORTBY", new SORTBY("SORTBY"));
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
    functions.put("MATCH", new MATCH("MATCH"));
    functions.put("MATCHER", new MATCHER("MATCHER"));
    functions.put("REPLACE", new REPLACE("REPLACE", false));
    functions.put("REPLACEALL", new REPLACE("REPLACEALL", true));
    
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8)) {
      functions.put("TEMPLATE", new TEMPLATE("TEMPLATE"));
      functions.put("TOTIMESTAMP", new TOTIMESTAMP("TOTIMESTAMP"));
    } else {
      functions.put("TEMPLATE", new FAIL("TEMPLATE requires JAVA 1.8+"));
      functions.put("TOTIMESTAMP", new FAIL("TOTIMESTAMP requires JAVA 1.8+"));
    }

    functions.put("DISCORDS", new DISCORDS("DISCORDS", true));
    functions.put("ZDISCORDS", new DISCORDS("ZDISCORDS", false));
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
    functions.put("WRAPRAW", new WRAPRAW("WRAPRAW"));
    functions.put("WRAPOPT", new WRAP("WRAPOPT", true));
    functions.put("WRAPRAWOPT", new WRAPRAW("WRAPRAWOPT", true));
    functions.put(UNWRAP, new UNWRAP(UNWRAP));
    functions.put("UNWRAPEMPTY", new UNWRAP("UNWRAPEMPTY", true));
    functions.put("UNWRAPSIZE", new UNWRAPSIZE("UNWRAPSIZE"));
    
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
    
    functions.put("ATINDEX", new ATINDEX("ATINDEX"));
    functions.put("ATTICK", new ATTICK("ATTICK"));
    functions.put("ATBUCKET", new ATBUCKET("ATBUCKET"));
    
    functions.put("CLONE", new CLONE("CLONE"));
    functions.put("DURATION", new DURATION("DURATION"));
    functions.put("HUMANDURATION", new HUMANDURATION("HUMANDURATION"));
    functions.put("ISODURATION", new ISODURATION("ISODURATION"));
    functions.put("ISO8601", new ISO8601("ISO8601"));
    functions.put("NOTBEFORE", new NOTBEFORE("NOTBEFORE"));
    functions.put("NOTAFTER", new NOTAFTER("NOTAFTER"));
    functions.put("TSELEMENTS", new TSELEMENTS("TSELEMENTS"));
    functions.put("->TSELEMENTS", new TSELEMENTS("->TSELEMENTS"));
    functions.put("TSELEMENTS->", new FROMTSELEMENTS("TSELEMENTS->"));
    functions.put("ADDDAYS", new ADDDAYS("ADDDAYS"));
    functions.put("ADDMONTHS", new ADDMONTHS("ADDMONTHS"));
    functions.put("ADDYEARS", new ADDYEARS("ADDYEARS"));
    
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
    functions.put(NULL, new NULL(NULL));
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
    functions.put("mapper.parsedouble", new MapperParseDouble.Builder("mapper.parsedouble"));
    
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
    functions.put("filter.bylabels", new FilterByLabels.Builder("filter.bylabels", true, false));
    functions.put("filter.byattr", new FilterByLabels.Builder("filter.byattr", false, true));
    functions.put("filter.bylabelsattr", new FilterByLabels.Builder("filter.bylabelsattr", true, true));
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
    
    functions.put("->HHCODE", new TOHHCODE("->HHCODE", true));
    functions.put("->HHCODELONG", new TOHHCODE("->HHCODELONG", false));
    functions.put("HHCODE->", new HHCODETO("HHCODE->"));
    functions.put("GEO.REGEXP", new GEOREGEXP("GEO.REGEXP"));
    functions.put(GEO_WKT, new GeoWKT(GEO_WKT));
    functions.put(GEO_INTERSECTION, new GeoIntersection(GEO_INTERSECTION));
    functions.put(GEO_UNION, new GeoUnion(GEO_UNION));
    functions.put(GEO_DIFFERENCE, new GeoSubtraction(GEO_DIFFERENCE));
    functions.put("GEO.WITHIN", new GEOWITHIN("GEO.WITHIN"));
    functions.put("GEO.INTERSECTS", new GEOINTERSECTS("GEO.WINTERSECTS"));
    functions.put("HAVERSINE", new HAVERSINE("HAVERSINE"));
    functions.put(GEOPACK, new GEOPACK(GEOPACK));
    functions.put(GEOUNPACK, new GEOUNPACK(GEOUNPACK));
    functions.put("mapper.geo.within", new MapperGeoWithin.Builder("mapper.geo.within"));
    functions.put("mapper.geo.outside", new MapperGeoOutside.Builder("mapper.geo.outside"));
    functions.put("mapper.geo.approximate", new MapperGeoApproximate.Builder("mapper.geo.approximate"));
    functions.put("COPYGEO", new COPYGEO("COPYGEO"));
    functions.put("BBOX", new BBOX("BBOX"));
    functions.put("->GEOHASH", new TOGEOHASH("->GEOHASH"));
    functions.put("GEOHASH->", new GEOHASHTO("GEOHASH->"));
    
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
    functions.put("LR", new LR("LR"));
    functions.put("MODE", new MODE("MODE"));
    
    functions.put("->Z", new TOZ("->Z"));
    functions.put("Z->", new ZTO("Z->"));
    functions.put("PACK", new PACK("PACK"));
    functions.put("UNPACK", new UNPACK("UNPACK"));
    
    //
    // Linear Algebra
    //
    
    functions.put("->MAT", new TOMAT("->MAT"));
    functions.put("MAT->", new MATTO("MAT->"));
    functions.put("TR", new TR("TR"));
    functions.put("TRANSPOSE", new TRANSPOSE("TRANSPOSE"));
    functions.put("DET", new DET("DET"));
    functions.put("INV", new INV("INV"));
    functions.put("->VEC", new TOVEC("->VEC"));
    functions.put("VEC->", new VECTO("VEC->"));
    
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
    // Moved from JavaLibrary
    //
    /////////////////////////
    
    //
    // Bucketizers
    //

    functions.put("bucketizer.and", new And("bucketizer.and", false));
    functions.put("bucketizer.first", new First("bucketizer.first"));
    functions.put("bucketizer.last", new Last("bucketizer.last"));
    functions.put("bucketizer.min", new Min("bucketizer.min", true));
    functions.put("bucketizer.max", new Max("bucketizer.max", true));
    functions.put("bucketizer.mean", new Mean("bucketizer.mean", false));
    functions.put("bucketizer.median", new Median("bucketizer.median"));
    functions.put("bucketizer.mad", new MAD("bucketizer.mad"));
    functions.put("bucketizer.or", new Or("bucketizer.or", false));
    functions.put("bucketizer.sum", new Sum("bucketizer.sum", true));
    functions.put("bucketizer.join", new Join.Builder("bucketizer.join", true, false, null));
    functions.put("bucketizer.count", new Count("bucketizer.count", false));
    functions.put("bucketizer.percentile", new Percentile.Builder("bucketizer.percentile"));
    functions.put("bucketizer.min.forbid-nulls", new Min("bucketizer.min.forbid-nulls", false));
    functions.put("bucketizer.max.forbid-nulls", new Max("bucketizer.max.forbid-nulls", false));
    functions.put("bucketizer.mean.exclude-nulls", new Mean("bucketizer.mean.exclude-nulls", true));
    functions.put("bucketizer.sum.forbid-nulls", new Sum("bucketizer.sum.forbid-nulls", false));
    functions.put("bucketizer.join.forbid-nulls", new Join.Builder("bucketizer.join.forbid-nulls", false, false, null));
    functions.put("bucketizer.count.exclude-nulls", new Count("bucketizer.count.exclude-nulls", true));
    functions.put("bucketizer.count.include-nulls", new Count("bucketizer.count.include-nulls", false));
    functions.put("bucketizer.count.nonnull", new Count("bucketizer.count.nonnull", true));
    functions.put("bucketizer.mean.circular", new CircularMean.Builder("bucketizer.mean.circular", true));
    functions.put("bucketizer.mean.circular.exclude-nulls", new CircularMean.Builder("bucketizer.mean.circular.exclude-nulls", false));
    //
    // Mappers
    //

    functions.put("mapper.and", new And("mapper.and", false));
    functions.put("mapper.count", new Count("mapper.count", false));
    functions.put("mapper.first", new First("mapper.first"));
    functions.put("mapper.last", new Last("mapper.last"));
    functions.put(MAPPER_MIN, new Min(MAPPER_MIN, true));
    functions.put(MAPPER_MAX, new Max(MAPPER_MAX, true));
    functions.put("mapper.mean", new Mean("mapper.mean", false));
    functions.put("mapper.median", new Median("mapper.median"));
    functions.put("mapper.mad", new MAD("mapper.mad"));
    functions.put("mapper.or", new Or("mapper.or", false));
    functions.put(MAPPER_HIGHEST, new Highest(MAPPER_HIGHEST));
    functions.put(MAPPER_LOWEST, new Lowest(MAPPER_LOWEST));
    functions.put("mapper.sum", new Sum("mapper.sum", true));
    functions.put("mapper.join", new Join.Builder("mapper.join", true, false, null));
    functions.put("mapper.delta", new Delta("mapper.delta"));
    functions.put("mapper.rate", new Rate("mapper.rate"));
    functions.put("mapper.hspeed", new HSpeed("mapper.hspeed"));
    functions.put("mapper.hdist", new HDist("mapper.hdist"));
    functions.put("mapper.truecourse", new TrueCourse("mapper.truecourse"));
    functions.put("mapper.vspeed", new VSpeed("mapper.vspeed"));
    functions.put("mapper.vdist", new VDist("mapper.vdist"));
    functions.put("mapper.var", new Variance.Builder("mapper.var", false));
    functions.put("mapper.sd", new StandardDeviation.Builder("mapper.sd", false));
    functions.put("mapper.abs", new MapperAbs("mapper.abs"));
    functions.put("mapper.ceil", new MapperCeil("mapper.ceil"));
    functions.put("mapper.floor", new MapperFloor("mapper.floor"));
    functions.put("mapper.finite", new MapperFinite("mapper.finite"));
    functions.put("mapper.round", new MapperRound("mapper.round"));
    functions.put("mapper.toboolean", new MapperToBoolean("mapper.toboolean"));
    functions.put("mapper.tolong", new MapperToLong("mapper.tolong"));
    functions.put("mapper.todouble", new MapperToDouble("mapper.todouble"));
    functions.put("mapper.tostring", new MapperToString("mapper.tostring"));
    functions.put("mapper.tanh", new MapperTanh("mapper.tanh"));
    functions.put("mapper.sigmoid", new MapperSigmoid("mapper.sigmoid"));
    functions.put("mapper.product", new MapperProduct("mapper.product"));
    functions.put("mapper.geo.clear", new MapperGeoClearPosition("mapper.geo.clear"));
    functions.put("mapper.count.exclude-nulls", new Count("mapper.count.exclude-nulls", true));
    functions.put("mapper.count.include-nulls", new Count("mapper.count.include-nulls", false));
    functions.put("mapper.count.nonnull", new Count("mapper.count.nonnull", true));
    functions.put("mapper.min.forbid-nulls", new Min("mapper.min.forbid-nulls", false));
    functions.put("mapper.max.forbid-nulls", new Max("mapper.max.forbid-nulls", false));
    functions.put("mapper.mean.exclude-nulls", new Mean("mapper.mean.exclude-nulls", true));
    functions.put("mapper.sum.forbid-nulls", new Sum("mapper.sum.forbid-nulls", false));
    functions.put("mapper.join.forbid-nulls", new Join.Builder("mapper.join.forbid-nulls", false, false, null));
    functions.put("mapper.var.forbid-nulls", new Variance.Builder("mapper.var.forbid-nulls", true));
    functions.put("mapper.sd.forbid-nulls", new StandardDeviation.Builder("mapper.sd.forbid-nulls", true));
    functions.put("mapper.mean.circular", new CircularMean.Builder("mapper.mean.circular", true));
    functions.put("mapper.mean.circular.exclude-nulls", new CircularMean.Builder("mapper.mean.circular.exclude-nulls", false));
    functions.put("mapper.mod", new MapperMod.Builder("mapper.mod"));
    
    //
    // Reducers
    //

    functions.put("reducer.and", new And("reducer.and", false));
    functions.put("reducer.and.exclude-nulls", new And("reducer.and.exclude-nulls", true));
    functions.put("reducer.min", new Min("reducer.min", true));
    functions.put("reducer.min.forbid-nulls", new Min("reducer.min.forbid-nulls", false));
    functions.put("reducer.min.nonnull", new Min("reducer.min.nonnull", false));
    functions.put("reducer.max", new Max("reducer.max", true));
    functions.put("reducer.max.forbid-nulls", new Max("reducer.max.forbid-nulls", false));
    functions.put("reducer.max.nonnull", new Max("reducer.max.nonnull", false));
    functions.put("reducer.mean", new Mean("reducer.mean", false));
    functions.put("reducer.mean.exclude-nulls", new Mean("reducer.mean.exclude-nulls", true));
    functions.put("reducer.median", new Median("reducer.median"));
    functions.put("reducer.mad", new MAD("reducer.mad"));
    functions.put("reducer.or", new Or("reducer.or", false));
    functions.put("reducer.or.exclude-nulls", new Or("reducer.or.exclude-nulls", true));
    functions.put("reducer.sum", new Sum("reducer.sum", true));
    functions.put("reducer.sum.forbid-nulls", new Sum("reducer.sum.forbid-nulls", false));
    functions.put("reducer.sum.nonnull", new Sum("reducer.sum.nonnull", false));
    functions.put("reducer.join", new Join.Builder("reducer.join", true, false, null));
    functions.put("reducer.join.forbid-nulls", new Join.Builder("reducer.join.forbid-nulls", false, false, null));
    functions.put("reducer.join.nonnull", new Join.Builder("reducer.join.nonnull", false, false, null));
    functions.put("reducer.join.urlencoded", new Join.Builder("reducer.join.urlencoded", false, true, ""));
    functions.put("reducer.var", new Variance.Builder("reducer.var", false));
    functions.put("reducer.var.forbid-nulls", new Variance.Builder("reducer.var.forbid-nulls", false));
    functions.put("reducer.sd", new StandardDeviation.Builder("reducer.sd", false));
    functions.put("reducer.sd.forbid-nulls", new StandardDeviation.Builder("reducer.sd.forbid-nulls", false));
    functions.put("reducer.argmin", new Argmin.Builder("reducer.argmin"));
    functions.put("reducer.argmax", new Argmax.Builder("reducer.argmax"));
    functions.put("reducer.product", new MapperProduct("reducer.product"));
    functions.put("reducer.count", new Count("reducer.count", false));
    functions.put("reducer.count.include-nulls", new Count("reducer.count.include-nulls", false));
    functions.put("reducer.count.exclude-nulls", new Count("reducer.count.exclude-nulls", true));
    functions.put("reducer.count.nonnull", new Count("reducer.count.nonnull", true));
    functions.put("reducer.shannonentropy.0", new ShannonEntropy("reducer.shannonentropy.0", false));
    functions.put("reducer.shannonentropy.1", new ShannonEntropy("reducer.shannonentropy.1", true));
    functions.put("reducer.percentile", new Percentile.Builder("reducer.percentile"));
    functions.put("reducer.mean.circular", new CircularMean.Builder("reducer.mean.circular", true));
    functions.put("reducer.mean.circular.exclude-nulls", new CircularMean.Builder("reducer.mean.circular.exclude-nulls", false));
    
    //
    // Filters
    //
    
    //
    // N-ary ops
    //
    
    functions.put("op.add", new OpAdd("op.add", true));
    functions.put("op.add.ignore-nulls", new OpAdd("op.add.ignore-nulls", false));
    functions.put("op.sub", new OpSub("op.sub"));
    functions.put("op.mul", new OpMul("op.mul", true));
    functions.put("op.mul.ignore-nulls", new OpMul("op.mul.ignore-nulls", false));
    functions.put("op.div", new OpDiv("op.div"));
    functions.put("op.mask", new OpMask("op.mask", false));
    functions.put("op.negmask", new OpMask("op.negmask", true)); 
    functions.put("op.ne", new OpNE("op.ne"));
    functions.put("op.eq", new OpEQ("op.eq"));
    functions.put("op.lt", new OpLT("op.lt"));
    functions.put("op.gt", new OpGT("op.gt"));
    functions.put("op.le", new OpLE("op.le"));
    functions.put("op.ge", new OpGE("op.ge"));
    functions.put("op.and.ignore-nulls", new OpAND("op.and.ignore-nulls", false));
    functions.put("op.and", new OpAND("op.and", true));
    functions.put("op.or.ignore-nulls", new OpOR("op.or.ignore-nulls", false));
    functions.put("op.or", new OpOR("op.or", true));

    /////////////////////////
    
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
        register(new RWarpScriptExtension());
      }      
      if (lang.contains("JS")) {
        register(new JSWarpScriptExtension());
      }      
      if (lang.contains("GROOVY")) {
        register(new GroovyWarpScriptExtension());
      }      
      if (lang.contains("LUA")) {
        register(new LUAWarpScriptExtension());
      }      
      if (lang.contains("RUBY")) {
        register(new RubyWarpScriptExtension());
      }
      if (lang.contains("PYTHON")) {
        register(new PythonWarpScriptExtension());
      }
    }    
  }
  
  public static Object getFunction(String name) {
    return functions.get(name);
  }
  
  public static void registerExtensions() { 
    Properties props = WarpConfig.getProperties();
    
    if (null != props && props.containsKey(Configuration.CONFIG_WARPSCRIPT_EXTENSIONS)) {
      String[] extensions = props.getProperty(Configuration.CONFIG_WARPSCRIPT_EXTENSIONS).split(",");
      Set<String> ext = new HashSet<String>();
      
      for (String extension: extensions) {
        ext.add(extension.trim());
      }
      
      boolean failedExt = false;
      
      //
      // Determine the possible jar from which WarpScriptLib was loaded
      //
      
      String wsljar = null;
      URL wslurl = WarpScriptLib.class.getResource('/' + WarpScriptLib.class.getCanonicalName().replace('.',  '/') + ".class");
      if (null != wslurl && "jar".equals(wslurl.getProtocol())) {
        wsljar = wslurl.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");
      }
      
      for (String extension: ext) {
        try {
          //
          // Locate the class using the current class loader
          //
          
          URL url = WarpScriptLib.class.getResource('/' + extension.replace('.', '/') + ".class");
          
          if (null == url) {
            LOG.error("Unable to load extension '" + extension + "', make sure it is in the class path.");
            failedExt = true;
            continue;
          }
          
          Class cls = null;

          //
          // If the class was located in a jar, load it using a specific class loader
          // so we can have fat jars with specific deps, unless the jar is the same as
          // the one from which WarpScriptLib was loaded, in which case we use the same
          // class loader.
          //
          
          if ("jar".equals(url.getProtocol())) {
            String jarfile = url.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");

            ClassLoader cl = WarpScriptLib.class.getClassLoader();
            
            // If the jar differs from that from which WarpScriptLib was loaded, create a dedicated class loader
            if (!jarfile.equals(wsljar)) {
              cl = new WarpClassLoader(jarfile, WarpScriptLib.class.getClassLoader());
            }
          
            cls = Class.forName(extension, true, cl);
          } else {
            cls = Class.forName(extension, true, WarpScriptLib.class.getClassLoader());
          }

          //Class cls = Class.forName(extension);
          WarpScriptExtension wse = (WarpScriptExtension) cls.newInstance();          
          wse.register();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      
      if (failedExt) {
        throw new RuntimeException("Some WarpScript extensions could not be loaded, aborting.");
      }
    }
  }
  
  public static void register(WarpScriptExtension extension) {
    Map<String,Object> extfuncs = extension.getFunctions();
    
    if (null == extfuncs) {
      return;
    }
    
    for (Entry<String,Object> entry: extfuncs.entrySet()) {
      if (null == entry.getValue()) {
        functions.remove(entry.getKey());
      } else {
        functions.put(entry.getKey(), entry.getValue());
      }
    }          
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
