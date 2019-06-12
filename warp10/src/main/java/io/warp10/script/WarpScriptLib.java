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

package io.warp10.script;

import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
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

import io.warp10.WarpClassLoader;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
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
import io.warp10.script.aggregator.Argminmax;
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
import io.warp10.script.aggregator.RMS;
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
import io.warp10.script.filler.FillerInterpolate;
import io.warp10.script.filler.FillerNext;
import io.warp10.script.filler.FillerPrevious;
import io.warp10.script.filler.FillerTrend;
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
import io.warp10.script.functions.math.ACOS;
import io.warp10.script.functions.math.ADDEXACT;
import io.warp10.script.functions.math.ASIN;
import io.warp10.script.functions.math.ATAN;
import io.warp10.script.functions.math.ATAN2;
import io.warp10.script.functions.math.CBRT;
import io.warp10.script.functions.math.CEIL;
import io.warp10.script.functions.math.COPYSIGN;
import io.warp10.script.functions.math.COS;
import io.warp10.script.functions.math.COSH;
import io.warp10.script.functions.math.DECREMENTEXACT;
import io.warp10.script.functions.math.EXP;
import io.warp10.script.functions.math.EXPM1;
import io.warp10.script.functions.math.FLOOR;
import io.warp10.script.functions.math.FLOORDIV;
import io.warp10.script.functions.math.FLOORMOD;
import io.warp10.script.functions.math.GETEXPONENT;
import io.warp10.script.functions.math.HYPOT;
import io.warp10.script.functions.math.IEEEREMAINDER;
import io.warp10.script.functions.math.INCREMENTEXACT;
import io.warp10.script.functions.math.LOG;
import io.warp10.script.functions.math.LOG10;
import io.warp10.script.functions.math.LOG1P;
import io.warp10.script.functions.math.MAX;
import io.warp10.script.functions.math.MIN;
import io.warp10.script.functions.math.MULTIPLYEXACT;
import io.warp10.script.functions.math.NEGATEEXACT;
import io.warp10.script.functions.math.NEXTAFTER;
import io.warp10.script.functions.math.NEXTDOWN;
import io.warp10.script.functions.math.NEXTUP;
import io.warp10.script.functions.math.RANDOM;
import io.warp10.script.functions.math.RINT;
import io.warp10.script.functions.math.ROUND;
import io.warp10.script.functions.math.SCALB;
import io.warp10.script.functions.math.SIGNUM;
import io.warp10.script.functions.math.SIN;
import io.warp10.script.functions.math.SINH;
import io.warp10.script.functions.math.SQRT;
import io.warp10.script.functions.math.SUBTRACTEXACT;
import io.warp10.script.functions.math.TAN;
import io.warp10.script.functions.math.TANH;
import io.warp10.script.functions.math.TODEGREES;
import io.warp10.script.functions.math.TOINTEXACT;
import io.warp10.script.functions.math.TORADIANS;
import io.warp10.script.functions.math.ULP;
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
import io.warp10.script.processing.image.Pfilter;
import io.warp10.script.processing.image.Pget;
import io.warp10.script.processing.image.Pimage;
import io.warp10.script.processing.image.PimageMode;
import io.warp10.script.processing.image.PnoTint;
import io.warp10.script.processing.image.Ppixels;
import io.warp10.script.processing.image.Pset;
import io.warp10.script.processing.image.Ptint;
import io.warp10.script.processing.image.PtoImage;
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
import io.warp10.script.processing.shape.PloadShape;
import io.warp10.script.processing.shape.Ppoint;
import io.warp10.script.processing.shape.Pquad;
import io.warp10.script.processing.shape.PquadraticVertex;
import io.warp10.script.processing.shape.Prect;
import io.warp10.script.processing.shape.PrectMode;
import io.warp10.script.processing.shape.Pshape;
import io.warp10.script.processing.shape.PshapeMode;
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

/**
 * Library of functions used to manipulate Geo Time Series
 * and more generally interact with a WarpScriptStack
 */
public class WarpScriptLib {
  
  private static final Logger LOG = LoggerFactory.getLogger(WarpScriptLib.class);
  
  private static Map<String,Object> functions = new HashMap<String, Object>();
  
  private static Set<String> extloaded = new LinkedHashSet<String>();
  
  /**
   * Static definition of name so it can be reused outside of WarpScriptLib
   */
  
  public static final String NULL = "NULL";

  public static final String COUNTER = "COUNTER";
  public static final String COUNTERSET = "COUNTERSET";
  
  
  public static final String REF = "REF";
  public static final String COMPILE = "COMPILE";
  public static final String SAFECOMPILE = "SAFECOMPILE";
  public static final String COMPILED = "COMPILED";
  
  public static final String EVAL = "EVAL";
  public static final String EVALSECURE = "EVALSECURE";
  public static final String SNAPSHOT = "SNAPSHOT";
  public static final String SNAPSHOTALL = "SNAPSHOTALL";
  public static final String LOAD = "LOAD";
  public static final String POPR = "POPR";
  public static final String CPOPR = "CPOPR";
  public static final String PUSHR = "PUSHR";
  public static final String CLEARREGS = "CLEARREGS";
  public static final String RUN = "RUN";
  public static final String BOOTSTRAP = "BOOTSTRAP";
  public static final String NOOP = "NOOP";
  
  public static final String MAP_START = "{";
  public static final String MAP_END = "}";

  public static final String LIST_START = "[";
  public static final String LIST_END = "]";

  public static final String SET_START = "(";
  public static final String SET_END = ")";
  
  public static final String VECTOR_START = "[[";
  public static final String VECTOR_END = "]]";
  
  public static final String TO_VECTOR = "->V";
  public static final String TO_SET = "->SET";
  
  public static final String NEWGTS = "NEWGTS";
  public static final String SWAP = "SWAP";
  public static final String RELABEL = "RELABEL";
  public static final String RENAME = "RENAME";
  public static final String PARSESELECTOR = "PARSESELECTOR";
  
  public static final String GEO_WKT = "GEO.WKT";
  public static final String GEO_WKT_UNIFORM = "GEO.WKT.UNIFORM";
  
  public static final String GEO_JSON = "GEO.JSON";
  public static final String GEO_JSON_UNIFORM = "GEO.JSON.UNIFORM";
  public static final String GEO_INTERSECTION = "GEO.INTERSECTION";
  public static final String GEO_DIFFERENCE = "GEO.DIFFERENCE";
  public static final String GEO_UNION = "GEO.UNION";
  public static final String GEOPACK = "GEOPACK";
  public static final String GEOUNPACK = "GEOUNPACK";
  
  public static final String SECTION = "SECTION";
  public static final String UNWRAP = "UNWRAP";
  public static final String UNWRAPENCODER = "UNWRAPENCODER";
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
  
  public static final String MSGFAIL = "MSGFAIL";
  
  public static final String INPLACEADD = "+!";
  public static final String PUT = "PUT";

  public static final String SAVE = "SAVE";
  public static final String RESTORE = "RESTORE";

  public static final String CHRONOSTART = "CHRONOSTART";
  public static final String CHRONOEND = "CHRONOEND";

  public static final String TRY = "TRY";
  public static final String RETHROW = "RETHROW";

  static {

    addNamedWarpScriptFunction(new REV("REV"));
    addNamedWarpScriptFunction(new REPORT("REPORT"));
    addNamedWarpScriptFunction(new MINREV("MINREV"));

    addNamedWarpScriptFunction(new MANAGERONOFF("UPDATEON", WarpManager.UPDATE_DISABLED, true));   
    addNamedWarpScriptFunction(new MANAGERONOFF("UPDATEOFF", WarpManager.UPDATE_DISABLED, false));   
    addNamedWarpScriptFunction(new MANAGERONOFF("METAON", WarpManager.META_DISABLED, true));   
    addNamedWarpScriptFunction(new MANAGERONOFF("METAOFF", WarpManager.META_DISABLED, false));   
    addNamedWarpScriptFunction(new MANAGERONOFF("DELETEON", WarpManager.DELETE_DISABLED, true));   
    addNamedWarpScriptFunction(new MANAGERONOFF("DELETEOFF", WarpManager.DELETE_DISABLED, false));
    
    addNamedWarpScriptFunction(new NOOP(BOOTSTRAP));

    addNamedWarpScriptFunction(new RTFM("RTFM"));
    addNamedWarpScriptFunction(new MAN("MAN"));

    //
    // Stack manipulation functions
    //
    
    addNamedWarpScriptFunction(new PIGSCHEMA("PIGSCHEMA"));
    addNamedWarpScriptFunction(new MARK(MARK));
    addNamedWarpScriptFunction(new CLEARTOMARK("CLEARTOMARK"));
    addNamedWarpScriptFunction(new COUNTTOMARK("COUNTTOMARK"));
    addNamedWarpScriptFunction(new AUTHENTICATE("AUTHENTICATE"));
    addNamedWarpScriptFunction(new ISAUTHENTICATED("ISAUTHENTICATED"));
    addNamedWarpScriptFunction(new STACKATTRIBUTE("STACKATTRIBUTE")); // NOT TO BE DOCUMENTED
    addNamedWarpScriptFunction(new EXPORT("EXPORT"));
    addNamedWarpScriptFunction(new TIMINGS("TIMINGS")); // NOT TO BE DOCUMENTED (YET)
    addNamedWarpScriptFunction(new NOTIMINGS("NOTIMINGS")); // NOT TO BE DOCUMENTED (YET)
    addNamedWarpScriptFunction(new ELAPSED("ELAPSED")); // NOT TO BE DOCUMENTED (YET)
    addNamedWarpScriptFunction(new TIMED("TIMED"));
    addNamedWarpScriptFunction(new CHRONOSTART(CHRONOSTART));
    addNamedWarpScriptFunction(new CHRONOEND(CHRONOEND));
    addNamedWarpScriptFunction(new CHRONOSTATS("CHRONOSTATS"));
    addNamedWarpScriptFunction(new TOLIST("->LIST"));
    addNamedWarpScriptFunction(new LISTTO("LIST->"));
    addNamedWarpScriptFunction(new UNLIST("UNLIST"));
    addNamedWarpScriptFunction(new TOSET(TO_SET));
    addNamedWarpScriptFunction(new SETTO("SET->"));
    addNamedWarpScriptFunction(new TOVECTOR(TO_VECTOR));
    addNamedWarpScriptFunction(new VECTORTO("V->"));
    addNamedWarpScriptFunction(new UNION("UNION"));
    addNamedWarpScriptFunction(new INTERSECTION("INTERSECTION"));
    addNamedWarpScriptFunction(new DIFFERENCE("DIFFERENCE"));
    addNamedWarpScriptFunction(new TOMAP("->MAP"));
    addNamedWarpScriptFunction(new MAPTO("MAP->"));
    addNamedWarpScriptFunction(new UNMAP("UNMAP"));
    addNamedWarpScriptFunction(new MAPID("MAPID"));
    addNamedWarpScriptFunction(new TOJSON("->JSON"));
    addNamedWarpScriptFunction(new JSONTO("JSON->"));
    addNamedWarpScriptFunction(new TOPICKLE("->PICKLE"));
    addNamedWarpScriptFunction(new PICKLETO("PICKLE->"));
    addNamedWarpScriptFunction(new GET("GET"));
    addNamedWarpScriptFunction(new SET("SET"));
    addNamedWarpScriptFunction(new PUT(PUT));
    addNamedWarpScriptFunction(new SUBMAP("SUBMAP"));
    addNamedWarpScriptFunction(new SUBLIST("SUBLIST"));
    addNamedWarpScriptFunction(new KEYLIST("KEYLIST"));
    addNamedWarpScriptFunction(new VALUELIST("VALUELIST"));
    addNamedWarpScriptFunction(new SIZE("SIZE"));
    addNamedWarpScriptFunction(new SHRINK("SHRINK"));
    addNamedWarpScriptFunction(new REMOVE("REMOVE"));
    addNamedWarpScriptFunction(new UNIQUE("UNIQUE"));
    addNamedWarpScriptFunction(new CONTAINS("CONTAINS"));
    addNamedWarpScriptFunction(new CONTAINSKEY("CONTAINSKEY"));
    addNamedWarpScriptFunction(new CONTAINSVALUE("CONTAINSVALUE"));
    addNamedWarpScriptFunction(new REVERSE("REVERSE", true));
    addNamedWarpScriptFunction(new REVERSE("CLONEREVERSE", false));
    addNamedWarpScriptFunction(new DUP("DUP"));
    addNamedWarpScriptFunction(new DUPN("DUPN"));
    addNamedWarpScriptFunction(new SWAP(SWAP));
    addNamedWarpScriptFunction(new DROP("DROP"));
    addNamedWarpScriptFunction(new SAVE(SAVE));
    addNamedWarpScriptFunction(new RESTORE(RESTORE));
    addNamedWarpScriptFunction(new CLEAR("CLEAR"));
    addNamedWarpScriptFunction(new CLEARDEFS("CLEARDEFS"));
    addNamedWarpScriptFunction(new CLEARSYMBOLS("CLEARSYMBOLS"));
    addNamedWarpScriptFunction(new DROPN("DROPN"));
    addNamedWarpScriptFunction(new ROT("ROT"));
    addNamedWarpScriptFunction(new ROLL("ROLL"));
    addNamedWarpScriptFunction(new ROLLD("ROLLD"));
    addNamedWarpScriptFunction(new PICK("PICK"));
    addNamedWarpScriptFunction(new DEPTH("DEPTH"));
    addNamedWarpScriptFunction(new MAXDEPTH("MAXDEPTH"));
    addNamedWarpScriptFunction(new RESET("RESET"));
    addNamedWarpScriptFunction(new MAXOPS("MAXOPS"));
    addNamedWarpScriptFunction(new MAXLOOP("MAXLOOP"));
    addNamedWarpScriptFunction(new MAXBUCKETS("MAXBUCKETS"));
    addNamedWarpScriptFunction(new MAXGEOCELLS("MAXGEOCELLS"));
    addNamedWarpScriptFunction(new MAXPIXELS("MAXPIXELS"));
    addNamedWarpScriptFunction(new MAXRECURSION("MAXRECURSION"));
    addNamedWarpScriptFunction(new OPS("OPS"));
    addNamedWarpScriptFunction(new MAXSYMBOLS("MAXSYMBOLS"));
    addNamedWarpScriptFunction(new EVAL(EVAL));
    addNamedWarpScriptFunction(new NOW("NOW"));
    addNamedWarpScriptFunction(new AGO("AGO"));
    addNamedWarpScriptFunction(new MSTU("MSTU"));
    addNamedWarpScriptFunction(new STU("STU"));
    addNamedWarpScriptFunction(new APPEND("APPEND"));
    addNamedWarpScriptFunction(new STORE(STORE));
    addNamedWarpScriptFunction(new CSTORE("CSTORE"));
    addNamedWarpScriptFunction(new LOAD(LOAD));
    addNamedWarpScriptFunction(new IMPORT("IMPORT"));
    addNamedWarpScriptFunction(new RUN(RUN));
    addNamedWarpScriptFunction(new DEF("DEF"));
    addNamedWarpScriptFunction(new UDF("UDF", false));
    addNamedWarpScriptFunction(new UDF("CUDF", true));
    addNamedWarpScriptFunction(new CALL("CALL"));
    addNamedWarpScriptFunction(new FORGET("FORGET"));
    addNamedWarpScriptFunction(new DEFINED("DEFINED"));
    addNamedWarpScriptFunction(new REDEFS("REDEFS"));
    addNamedWarpScriptFunction(new DEFINEDMACRO("DEFINEDMACRO"));
    addNamedWarpScriptFunction(new DEFINEDMACRO("CHECKMACRO", true));
    addNamedWarpScriptFunction(new NaN("NaN"));
    addNamedWarpScriptFunction(new ISNaN("ISNaN"));
    addNamedWarpScriptFunction(new TYPEOF("TYPEOF"));
    addNamedWarpScriptFunction(new EXTLOADED("EXTLOADED"));
    addNamedWarpScriptFunction(new ASSERT("ASSERT"));
    addNamedWarpScriptFunction(new ASSERTMSG("ASSERTMSG"));
    addNamedWarpScriptFunction(new FAIL("FAIL"));
    addNamedWarpScriptFunction(new MSGFAIL(MSGFAIL));
    addNamedWarpScriptFunction(new STOP("STOP"));
    addNamedWarpScriptFunction(new TRY(TRY));
    addNamedWarpScriptFunction(new RETHROW(RETHROW));
    addNamedWarpScriptFunction(new ERROR("ERROR"));
    addNamedWarpScriptFunction(new TIMEBOX("TIMEBOX"));
    addNamedWarpScriptFunction(new JSONSTRICT("JSONSTRICT"));
    addNamedWarpScriptFunction(new JSONLOOSE("JSONLOOSE"));
    addNamedWarpScriptFunction(new DEBUGON("DEBUGON"));
    addNamedWarpScriptFunction(new NDEBUGON("NDEBUGON"));
    addNamedWarpScriptFunction(new DEBUGOFF("DEBUGOFF"));
    addNamedWarpScriptFunction(new LINEON("LINEON"));
    addNamedWarpScriptFunction(new LINEOFF("LINEOFF"));
    addNamedWarpScriptFunction(new LMAP("LMAP"));
    addNamedWarpScriptFunction(new NONNULL("NONNULL"));
    addNamedWarpScriptFunction(new LFLATMAP("LFLATMAP"));
    addNamedWarpScriptFunction(new EMPTYLIST("[]"));
    addNamedWarpScriptFunction(new MARK(LIST_START));
    addNamedWarpScriptFunction(new ENDLIST(LIST_END));
    addNamedWarpScriptFunction(new STACKTOLIST("STACKTOLIST"));
    addNamedWarpScriptFunction(new MARK(SET_START));
    addNamedWarpScriptFunction(new ENDSET(SET_END));
    addNamedWarpScriptFunction(new EMPTYSET("()"));
    addNamedWarpScriptFunction(new MARK(VECTOR_START));
    addNamedWarpScriptFunction(new ENDVECTOR(VECTOR_END));
    addNamedWarpScriptFunction(new EMPTYVECTOR("[[]]"));
    addNamedWarpScriptFunction(new EMPTYMAP("{}"));
    addNamedWarpScriptFunction(new IMMUTABLE("IMMUTABLE"));
    addNamedWarpScriptFunction(new MARK(MAP_START));
    addNamedWarpScriptFunction(new ENDMAP(MAP_END));
    addNamedWarpScriptFunction(new SECUREKEY("SECUREKEY"));
    addNamedWarpScriptFunction(new SECURE("SECURE"));
    addNamedWarpScriptFunction(new UNSECURE("UNSECURE", true));
    addNamedWarpScriptFunction(new EVALSECURE(EVALSECURE));
    addNamedWarpScriptFunction(new NOOP("NOOP"));
    addNamedWarpScriptFunction(new DOC("DOC"));
    addNamedWarpScriptFunction(new DOCMODE("DOCMODE"));
    addNamedWarpScriptFunction(new INFO("INFO"));
    addNamedWarpScriptFunction(new INFOMODE("INFOMODE"));
    addNamedWarpScriptFunction(new SECTION(SECTION));
    addNamedWarpScriptFunction(new GETSECTION("GETSECTION"));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOT, false, false, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTALL, true, false, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTTOMARK", false, true, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTALLTOMARK", true, true, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTCOPY", false, false, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTCOPYALL", true, false, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTCOPYTOMARK", false, true, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTCOPYALLTOMARK", true, true, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTN", false, false, true, true));
    addNamedWarpScriptFunction(new SNAPSHOT("SNAPSHOTCOPYN", false, false, false, true));
    addNamedWarpScriptFunction(new HEADER("HEADER"));
    
    addNamedWarpScriptFunction(new ECHOON("ECHOON"));
    addNamedWarpScriptFunction(new ECHOOFF("ECHOOFF"));
    addNamedWarpScriptFunction(new JSONSTACK("JSONSTACK"));
    addNamedWarpScriptFunction(new WSSTACK("WSSTACK"));
    addNamedWarpScriptFunction(new PEEK("PEEK"));
    addNamedWarpScriptFunction(new PEEKN("PEEKN"));
    addNamedWarpScriptFunction(new NPEEK("NPEEK"));
    addNamedWarpScriptFunction(new PSTACK("PSTACK"));
    addNamedWarpScriptFunction(new TIMEON("TIMEON"));
    addNamedWarpScriptFunction(new TIMEOFF("TIMEOFF"));

    //
    // Compilation related dummy functions
    //
    addNamedWarpScriptFunction(new FAIL(COMPILE, "Not supported"));
    addNamedWarpScriptFunction(new NOOP(SAFECOMPILE));
    addNamedWarpScriptFunction(new FAIL(COMPILED, "Not supported"));
    addNamedWarpScriptFunction(new REF(REF));

    addNamedWarpScriptFunction(new MACROTTL("MACROTTL"));
    addNamedWarpScriptFunction(new WFON("WFON"));
    addNamedWarpScriptFunction(new WFOFF("WFOFF"));
    addNamedWarpScriptFunction(new SETMACROCONFIG("SETMACROCONFIG"));
    addNamedWarpScriptFunction(new MACROCONFIGSECRET("MACROCONFIGSECRET"));
    addNamedWarpScriptFunction(new MACROCONFIG("MACROCONFIG", false));
    addNamedWarpScriptFunction(new MACROCONFIG("MACROCONFIGDEFAULT", true));
    addNamedWarpScriptFunction(new MACROMAPPER("MACROMAPPER"));
    addNamedWarpScriptFunction(new MACROMAPPER("MACROREDUCER"));
    addNamedWarpScriptFunction(new MACROMAPPER("MACROBUCKETIZER"));
    addNamedWarpScriptFunction(new MACROFILTER("MACROFILTER"));
    addNamedWarpScriptFunction(new MACROFILLER("MACROFILLER"));
    addNamedWarpScriptFunction(new STRICTMAPPER("STRICTMAPPER"));
    addNamedWarpScriptFunction(new STRICTREDUCER("STRICTREDUCER"));
    
    addNamedWarpScriptFunction(new PARSESELECTOR(PARSESELECTOR));
    addNamedWarpScriptFunction(new TOSELECTOR("TOSELECTOR"));
    addNamedWarpScriptFunction(new PARSE("PARSE"));
    addNamedWarpScriptFunction(new SMARTPARSE("SMARTPARSE"));
        
    // We do not expose DUMP, it might allocate too much memory
    //addNamedWarpScriptFunction(new DUMP("DUMP"));
    
    // Binary ops
    addNamedWarpScriptFunction(new ADD("+"));
    addNamedWarpScriptFunction(new INPLACEADD(INPLACEADD));
    addNamedWarpScriptFunction(new SUB("-"));
    addNamedWarpScriptFunction(new DIV("/"));
    addNamedWarpScriptFunction(new MUL("*"));
    addNamedWarpScriptFunction(new POW("**"));
    addNamedWarpScriptFunction(new MOD("%"));
    addNamedWarpScriptFunction(new EQ("=="));
    addNamedWarpScriptFunction(new NE("!="));
    addNamedWarpScriptFunction(new LT("<"));
    addNamedWarpScriptFunction(new GT(">"));
    addNamedWarpScriptFunction(new LE("<="));
    addNamedWarpScriptFunction(new GE(">="));
    addNamedWarpScriptFunction(new CondAND("&&"));
    addNamedWarpScriptFunction(new CondAND("AND"));
    addNamedWarpScriptFunction(new CondOR("||"));
    addNamedWarpScriptFunction(new CondOR("OR"));
    addNamedWarpScriptFunction(new BitwiseAND("&"));
    addNamedWarpScriptFunction(new SHIFTRIGHT(">>", true));
    addNamedWarpScriptFunction(new SHIFTRIGHT(">>>", false));
    addNamedWarpScriptFunction(new SHIFTLEFT("<<"));
    addNamedWarpScriptFunction(new BitwiseOR("|"));
    addNamedWarpScriptFunction(new BitwiseXOR("^"));
    addNamedWarpScriptFunction(new ALMOSTEQ("~="));

    // Bitset ops
    addNamedWarpScriptFunction(new BITGET("BITGET"));
    addNamedWarpScriptFunction(new BITCOUNT("BITCOUNT"));
    addNamedWarpScriptFunction(new BITSTOBYTES("BITSTOBYTES"));
    addNamedWarpScriptFunction(new BYTESTOBITS("BYTESTOBITS"));
    
    // Unary ops    
    addNamedWarpScriptFunction(new NOT("!"));
    addNamedWarpScriptFunction(new COMPLEMENT("~"));
    addNamedWarpScriptFunction(new REVERSEBITS("REVBITS"));
    addNamedWarpScriptFunction(new NOT("NOT"));
    addNamedWarpScriptFunction(new ABS("ABS"));
    addNamedWarpScriptFunction(new TODOUBLE("TODOUBLE"));
    addNamedWarpScriptFunction(new TOBOOLEAN("TOBOOLEAN"));
    addNamedWarpScriptFunction(new TOLONG("TOLONG"));
    addNamedWarpScriptFunction(new TOSTRING("TOSTRING"));
    addNamedWarpScriptFunction(new TOHEX("TOHEX"));
    addNamedWarpScriptFunction(new TOBIN("TOBIN"));
    addNamedWarpScriptFunction(new FROMHEX("FROMHEX"));
    addNamedWarpScriptFunction(new FROMBIN("FROMBIN"));
    addNamedWarpScriptFunction(new TOBITS("TOBITS", false));
    addNamedWarpScriptFunction(new FROMBITS("FROMBITS", false));
    addNamedWarpScriptFunction(new TOLONGBYTES("->LONGBYTES"));
    addNamedWarpScriptFunction(new TOBITS("->DOUBLEBITS", false));
    addNamedWarpScriptFunction(new FROMBITS("DOUBLEBITS->", false));
    addNamedWarpScriptFunction(new TOBITS("->FLOATBITS", true));
    addNamedWarpScriptFunction(new FROMBITS("FLOATBITS->", true));
    addNamedWarpScriptFunction(new TOKENINFO("TOKENINFO"));
    addNamedWarpScriptFunction(new GETHOOK("GETHOOK"));
    
    // Unit converters
    addNamedWarpScriptFunction(new UNIT("w", 7 * 24 * 60 * 60 * 1000));
    addNamedWarpScriptFunction(new UNIT("d", 24 * 60 * 60 * 1000));
    addNamedWarpScriptFunction(new UNIT("h", 60 * 60 * 1000));
    addNamedWarpScriptFunction(new UNIT("m", 60 * 1000));
    addNamedWarpScriptFunction(new UNIT("s",  1000));
    addNamedWarpScriptFunction(new UNIT("ms", 1));
    addNamedWarpScriptFunction(new UNIT("us", 0.001));
    addNamedWarpScriptFunction(new UNIT("ns", 0.000001));
    addNamedWarpScriptFunction(new UNIT("ps", 0.000000001));
    
    // Crypto functions
    addNamedWarpScriptFunction(new HASH("HASH"));
    addNamedWarpScriptFunction(new DIGEST("MD5", MD5Digest.class));
    addNamedWarpScriptFunction(new DIGEST("SHA1", SHA1Digest.class));
    addNamedWarpScriptFunction(new DIGEST("SHA256", SHA256Digest.class));
    addNamedWarpScriptFunction(new HMAC("SHA256HMAC", SHA256Digest.class));
    addNamedWarpScriptFunction(new HMAC("SHA1HMAC", SHA1Digest.class));
    addNamedWarpScriptFunction(new AESWRAP("AESWRAP"));
    addNamedWarpScriptFunction(new AESUNWRAP("AESUNWRAP"));
    addNamedWarpScriptFunction(new RUNNERNONCE("RUNNERNONCE"));
    addNamedWarpScriptFunction(new GZIP("GZIP"));
    addNamedWarpScriptFunction(new UNGZIP("UNGZIP"));
    addNamedWarpScriptFunction(new DEFLATE("DEFLATE"));
    addNamedWarpScriptFunction(new INFLATE("INFLATE"));
    addNamedWarpScriptFunction(new RSAGEN("RSAGEN"));
    addNamedWarpScriptFunction(new RSAPUBLIC(RSAPUBLIC));
    addNamedWarpScriptFunction(new RSAPRIVATE(RSAPRIVATE));
    addNamedWarpScriptFunction(new RSAENCRYPT("RSAENCRYPT"));
    addNamedWarpScriptFunction(new RSADECRYPT("RSADECRYPT"));
    addNamedWarpScriptFunction(new RSASIGN("RSASIGN"));
    addNamedWarpScriptFunction(new RSAVERIFY("RSAVERIFY"));

    //
    // String functions
    //
    
    addNamedWarpScriptFunction(new URLDECODE("URLDECODE"));
    addNamedWarpScriptFunction(new URLENCODE("URLENCODE"));
    addNamedWarpScriptFunction(new SPLIT("SPLIT"));
    addNamedWarpScriptFunction(new UUID("UUID"));
    addNamedWarpScriptFunction(new JOIN("JOIN"));
    addNamedWarpScriptFunction(new SUBSTRING("SUBSTRING"));
    addNamedWarpScriptFunction(new TOUPPER("TOUPPER"));
    addNamedWarpScriptFunction(new TOLOWER("TOLOWER"));
    addNamedWarpScriptFunction(new TRIM("TRIM"));
    
    addNamedWarpScriptFunction(new B64TOHEX("B64TOHEX"));
    addNamedWarpScriptFunction(new HEXTOB64("HEXTOB64"));
    addNamedWarpScriptFunction(new BINTOHEX("BINTOHEX"));
    addNamedWarpScriptFunction(new HEXTOBIN("HEXTOBIN"));
    
    addNamedWarpScriptFunction(new BINTO("BIN->"));
    addNamedWarpScriptFunction(new HEXTO("HEX->"));
    addNamedWarpScriptFunction(new B64TO("B64->"));
    addNamedWarpScriptFunction(new B64URLTO("B64URL->"));
    addNamedWarpScriptFunction(new BYTESTO(BYTESTO));

    addNamedWarpScriptFunction(new TOBYTES("->BYTES"));
    addNamedWarpScriptFunction(new io.warp10.script.functions.TOBIN("->BIN"));
    addNamedWarpScriptFunction(new io.warp10.script.functions.TOHEX("->HEX"));
    addNamedWarpScriptFunction(new TOB64("->B64"));
    addNamedWarpScriptFunction(new TOB64URL("->B64URL"));
    addNamedWarpScriptFunction(new TOOPB64(TOOPB64));
    addNamedWarpScriptFunction(new OPB64TO(OPB64TO));
    addNamedWarpScriptFunction(new OPB64TOHEX("OPB64TOHEX"));
    
    //
    // Conditionals
    //
    
    addNamedWarpScriptFunction(new IFT("IFT"));
    addNamedWarpScriptFunction(new IFTE("IFTE"));
    addNamedWarpScriptFunction(new SWITCH("SWITCH"));
    
    //
    // Loops
    //
    
    addNamedWarpScriptFunction(new WHILE("WHILE"));
    addNamedWarpScriptFunction(new UNTIL("UNTIL"));
    addNamedWarpScriptFunction(new FOR("FOR"));
    addNamedWarpScriptFunction(new FORSTEP("FORSTEP"));
    addNamedWarpScriptFunction(new FOREACH("FOREACH"));
    addNamedWarpScriptFunction(new BREAK("BREAK"));
    addNamedWarpScriptFunction(new CONTINUE("CONTINUE"));
    addNamedWarpScriptFunction(new EVERY("EVERY"));
    addNamedWarpScriptFunction(new RANGE("RANGE"));
    
    //
    // Macro end
    //
    
    addNamedWarpScriptFunction(new RETURN("RETURN"));
    addNamedWarpScriptFunction(new NRETURN("NRETURN"));
    
    //
    // GTS standalone functions
    //
    
    addNamedWarpScriptFunction(new NEWENCODER("NEWENCODER"));
    addNamedWarpScriptFunction(new CHUNKENCODER("CHUNKENCODER", true));
    addNamedWarpScriptFunction(new TOENCODER("->ENCODER"));
    addNamedWarpScriptFunction(new ENCODERTO("ENCODER->"));
    addNamedWarpScriptFunction(new TOGTS("->GTS"));
    addNamedWarpScriptFunction(new OPTIMIZE("OPTIMIZE"));
    addNamedWarpScriptFunction(new NEWGTS(NEWGTS));
    addNamedWarpScriptFunction(new MAKEGTS("MAKEGTS"));
    addNamedWarpScriptFunction(new ADDVALUE("ADDVALUE", false));
    addNamedWarpScriptFunction(new ADDVALUE("SETVALUE", true));
    addNamedWarpScriptFunction(new REMOVETICK("REMOVETICK"));
    addNamedWarpScriptFunction(new FETCH("FETCH", false, null));
    addNamedWarpScriptFunction(new FETCH("FETCHLONG", false, TYPE.LONG));
    addNamedWarpScriptFunction(new FETCH("FETCHDOUBLE", false, TYPE.DOUBLE));
    addNamedWarpScriptFunction(new FETCH("FETCHSTRING", false, TYPE.STRING));
    addNamedWarpScriptFunction(new FETCH("FETCHBOOLEAN", false, TYPE.BOOLEAN));
    addNamedWarpScriptFunction(new LIMIT("LIMIT"));
    addNamedWarpScriptFunction(new MAXGTS("MAXGTS"));
    addNamedWarpScriptFunction(new FIND("FIND", false));
    addNamedWarpScriptFunction(new FIND("FINDSETS", true));
    addNamedWarpScriptFunction(new FIND("METASET", false, true));
    addNamedWarpScriptFunction(new FINDSTATS("FINDSTATS"));
    addNamedWarpScriptFunction(new DEDUP("DEDUP"));
    addNamedWarpScriptFunction(new ONLYBUCKETS("ONLYBUCKETS"));
    addNamedWarpScriptFunction(new VALUEDEDUP("VALUEDEDUP"));
    addNamedWarpScriptFunction(new CLONEEMPTY("CLONEEMPTY"));
    addNamedWarpScriptFunction(new COMPACT("COMPACT"));
    addNamedWarpScriptFunction(new RANGECOMPACT("RANGECOMPACT"));
    addNamedWarpScriptFunction(new STANDARDIZE("STANDARDIZE"));
    addNamedWarpScriptFunction(new NORMALIZE("NORMALIZE"));
    addNamedWarpScriptFunction(new ISONORMALIZE("ISONORMALIZE"));
    addNamedWarpScriptFunction(new ZSCORE("ZSCORE"));
    addNamedWarpScriptFunction(new FILL("FILL"));
    addNamedWarpScriptFunction(new FILLPREVIOUS("FILLPREVIOUS"));
    addNamedWarpScriptFunction(new FILLNEXT("FILLNEXT"));
    addNamedWarpScriptFunction(new FILLVALUE("FILLVALUE"));
    addNamedWarpScriptFunction(new FILLTICKS("FILLTICKS"));
    addNamedWarpScriptFunction(new INTERPOLATE("INTERPOLATE"));
    addNamedWarpScriptFunction(new FIRSTTICK("FIRSTTICK"));
    addNamedWarpScriptFunction(new LASTTICK("LASTTICK"));
    addNamedWarpScriptFunction(new MERGE("MERGE"));
    addNamedWarpScriptFunction(new RESETS("RESETS"));
    addNamedWarpScriptFunction(new MONOTONIC("MONOTONIC"));
    addNamedWarpScriptFunction(new TIMESPLIT("TIMESPLIT"));
    addNamedWarpScriptFunction(new TIMECLIP("TIMECLIP"));
    addNamedWarpScriptFunction(new CLIP("CLIP"));
    addNamedWarpScriptFunction(new TIMEMODULO("TIMEMODULO"));
    addNamedWarpScriptFunction(new CHUNK("CHUNK", true));
    addNamedWarpScriptFunction(new FUSE("FUSE"));
    addNamedWarpScriptFunction(new RENAME(RENAME));
    addNamedWarpScriptFunction(new RELABEL(RELABEL));
    addNamedWarpScriptFunction(new SETATTRIBUTES("SETATTRIBUTES"));
    addNamedWarpScriptFunction(new CROP("CROP"));
    addNamedWarpScriptFunction(new TIMESHIFT("TIMESHIFT"));
    addNamedWarpScriptFunction(new TIMESCALE("TIMESCALE"));
    addNamedWarpScriptFunction(new TICKINDEX("TICKINDEX"));
    addNamedWarpScriptFunction(new FFT.Builder("FFT", true));
    addNamedWarpScriptFunction(new FFT.Builder("FFTAP", false));
    addNamedWarpScriptFunction(new IFFT.Builder("IFFT"));
    addNamedWarpScriptFunction(new FFTWINDOW("FFTWINDOW"));
    addNamedWarpScriptFunction(new FDWT("FDWT"));
    addNamedWarpScriptFunction(new IDWT("IDWT"));
    addNamedWarpScriptFunction(new DWTSPLIT("DWTSPLIT"));
    addNamedWarpScriptFunction(new EMPTY("EMPTY"));
    addNamedWarpScriptFunction(new NONEMPTY("NONEMPTY"));
    addNamedWarpScriptFunction(new PARTITION("PARTITION"));
    addNamedWarpScriptFunction(new PARTITION("STRICTPARTITION", true));
    addNamedWarpScriptFunction(new ZIP("ZIP"));
    addNamedWarpScriptFunction(new PATTERNS("PATTERNS", true));
    addNamedWarpScriptFunction(new PATTERNDETECTION("PATTERNDETECTION", true));
    addNamedWarpScriptFunction(new PATTERNS("ZPATTERNS", false));
    addNamedWarpScriptFunction(new PATTERNDETECTION("ZPATTERNDETECTION", false));
    addNamedWarpScriptFunction(new DTW("DTW", true, false));
    addNamedWarpScriptFunction(new OPTDTW("OPTDTW"));
    addNamedWarpScriptFunction(new DTW("ZDTW", true, true));
    addNamedWarpScriptFunction(new DTW("RAWDTW", false, false));
    addNamedWarpScriptFunction(new VALUEHISTOGRAM("VALUEHISTOGRAM"));
    addNamedWarpScriptFunction(new PROBABILITY.Builder("PROBABILITY"));
    addNamedWarpScriptFunction(new PROB("PROB"));
    addNamedWarpScriptFunction(new CPROB("CPROB"));
    addNamedWarpScriptFunction(new RANDPDF.Builder("RANDPDF"));
    addNamedWarpScriptFunction(new SINGLEEXPONENTIALSMOOTHING("SINGLEEXPONENTIALSMOOTHING"));
    addNamedWarpScriptFunction(new DOUBLEEXPONENTIALSMOOTHING("DOUBLEEXPONENTIALSMOOTHING"));
    addNamedWarpScriptFunction(new LOWESS("LOWESS"));
    addNamedWarpScriptFunction(new RLOWESS("RLOWESS"));
    addNamedWarpScriptFunction(new STL("STL"));
    addNamedWarpScriptFunction(new LTTB("LTTB", false));
    addNamedWarpScriptFunction(new LTTB("TLTTB", true));
    addNamedWarpScriptFunction(new LOCATIONOFFSET("LOCATIONOFFSET"));
    addNamedWarpScriptFunction(new FLATTEN("FLATTEN"));
    addNamedWarpScriptFunction(new RESHAPE("RESHAPE"));
    addNamedWarpScriptFunction(new SHAPE("SHAPE"));
    addNamedWarpScriptFunction(new CORRELATE.Builder("CORRELATE"));
    addNamedWarpScriptFunction(new SORT("SORT"));
    addNamedWarpScriptFunction(new SORTBY("SORTBY"));
    addNamedWarpScriptFunction(new RSORT("RSORT"));
    addNamedWarpScriptFunction(new LASTSORT("LASTSORT"));
    addNamedWarpScriptFunction(new METASORT("METASORT"));
    addNamedWarpScriptFunction(new VALUESORT("VALUESORT"));
    addNamedWarpScriptFunction(new RVALUESORT("RVALUESORT"));
    addNamedWarpScriptFunction(new LSORT("LSORT"));
    addNamedWarpScriptFunction(new SHUFFLE("SHUFFLE"));
    addNamedWarpScriptFunction(new MSORT("MSORT"));
    addNamedWarpScriptFunction(new GROUPBY("GROUPBY"));
    addNamedWarpScriptFunction(new FILTERBY("FILTERBY"));
    addNamedWarpScriptFunction(new UPDATE("UPDATE"));
    addNamedWarpScriptFunction(new META("META"));
    addNamedWarpScriptFunction(new DELETE("DELETE"));
    addNamedWarpScriptFunction(new WEBCALL("WEBCALL"));
    addNamedWarpScriptFunction(new MATCH("MATCH"));
    addNamedWarpScriptFunction(new MATCHER("MATCHER"));
    addNamedWarpScriptFunction(new REPLACE("REPLACE", false));
    addNamedWarpScriptFunction(new REPLACE("REPLACEALL", true));
    addNamedWarpScriptFunction(new REOPTALT("REOPTALT"));
    
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8)) {
      addNamedWarpScriptFunction(new TEMPLATE("TEMPLATE"));
      addNamedWarpScriptFunction(new TOTIMESTAMP("TOTIMESTAMP"));
    } else {
      addNamedWarpScriptFunction(new FAIL("TEMPLATE", "Requires JAVA 1.8+"));
      addNamedWarpScriptFunction(new FAIL("TOTIMESTAMP", "Requires JAVA 1.8+"));
    }

    addNamedWarpScriptFunction(new DISCORDS("DISCORDS", true));
    addNamedWarpScriptFunction(new DISCORDS("ZDISCORDS", false));
    addNamedWarpScriptFunction(new INTEGRATE("INTEGRATE"));
    
    addNamedWarpScriptFunction(new BUCKETSPAN("BUCKETSPAN"));
    addNamedWarpScriptFunction(new BUCKETCOUNT("BUCKETCOUNT"));
    addNamedWarpScriptFunction(new UNBUCKETIZE("UNBUCKETIZE"));
    addNamedWarpScriptFunction(new LASTBUCKET("LASTBUCKET"));
    addNamedWarpScriptFunction(new NAME("NAME"));
    addNamedWarpScriptFunction(new LABELS("LABELS"));
    addNamedWarpScriptFunction(new ATTRIBUTES("ATTRIBUTES"));
    addNamedWarpScriptFunction(new LASTACTIVITY("LASTACTIVITY"));
    addNamedWarpScriptFunction(new TICKS("TICKS"));
    addNamedWarpScriptFunction(new LOCATIONS("LOCATIONS"));
    addNamedWarpScriptFunction(new LOCSTRINGS("LOCSTRINGS"));
    addNamedWarpScriptFunction(new ELEVATIONS("ELEVATIONS"));
    addNamedWarpScriptFunction(new VALUES("VALUES"));
    addNamedWarpScriptFunction(new VALUESPLIT("VALUESPLIT"));
    addNamedWarpScriptFunction(new TICKLIST("TICKLIST"));
    addNamedWarpScriptFunction(new COMMONTICKS("COMMONTICKS"));
    addNamedWarpScriptFunction(new WRAP("WRAP"));
    addNamedWarpScriptFunction(new WRAPRAW("WRAPRAW"));
    addNamedWarpScriptFunction(new WRAPRAW("WRAPFAST", false, false));
    addNamedWarpScriptFunction(new WRAP("WRAPOPT", true));
    addNamedWarpScriptFunction(new WRAPRAW("WRAPRAWOPT", true));
    addNamedWarpScriptFunction(new UNWRAP(UNWRAP));
    addNamedWarpScriptFunction(new UNWRAP("UNWRAPEMPTY", true));
    addNamedWarpScriptFunction(new UNWRAPSIZE("UNWRAPSIZE"));
    addNamedWarpScriptFunction(new UNWRAPENCODER(UNWRAPENCODER));
    
    //
    // Outlier detection
    //
    
    addNamedWarpScriptFunction(new THRESHOLDTEST("THRESHOLDTEST"));
    addNamedWarpScriptFunction(new ZSCORETEST("ZSCORETEST"));
    addNamedWarpScriptFunction(new GRUBBSTEST("GRUBBSTEST"));
    addNamedWarpScriptFunction(new ESDTEST("ESDTEST"));
    addNamedWarpScriptFunction(new STLESDTEST("STLESDTEST"));
    addNamedWarpScriptFunction(new HYBRIDTEST("HYBRIDTEST"));
    addNamedWarpScriptFunction(new HYBRIDTEST2("HYBRIDTEST2"));
    
    //
    // Quaternion related functions
    //
    
    addNamedWarpScriptFunction(new TOQUATERNION("->Q"));
    addNamedWarpScriptFunction(new QUATERNIONTO("Q->"));
    addNamedWarpScriptFunction(new QCONJUGATE("QCONJUGATE"));
    addNamedWarpScriptFunction(new QDIVIDE("QDIVIDE"));
    addNamedWarpScriptFunction(new QMULTIPLY("QMULTIPLY"));
    addNamedWarpScriptFunction(new QROTATE("QROTATE"));
    addNamedWarpScriptFunction(new QROTATION("QROTATION"));
    addNamedWarpScriptFunction(new ROTATIONQ("ROTATIONQ"));
    
    addNamedWarpScriptFunction(new ATINDEX("ATINDEX"));
    addNamedWarpScriptFunction(new ATTICK("ATTICK"));
    addNamedWarpScriptFunction(new ATBUCKET("ATBUCKET"));
    
    addNamedWarpScriptFunction(new CLONE("CLONE"));
    addNamedWarpScriptFunction(new DURATION("DURATION"));
    addNamedWarpScriptFunction(new HUMANDURATION("HUMANDURATION"));
    addNamedWarpScriptFunction(new ISODURATION("ISODURATION"));
    addNamedWarpScriptFunction(new ISO8601("ISO8601"));
    addNamedWarpScriptFunction(new NOTBEFORE("NOTBEFORE"));
    addNamedWarpScriptFunction(new NOTAFTER("NOTAFTER"));
    addNamedWarpScriptFunction(new TSELEMENTS("TSELEMENTS"));
    addNamedWarpScriptFunction(new TSELEMENTS("->TSELEMENTS"));
    addNamedWarpScriptFunction(new FROMTSELEMENTS("TSELEMENTS->"));
    addNamedWarpScriptFunction(new ADDDAYS("ADDDAYS"));
    addNamedWarpScriptFunction(new ADDMONTHS("ADDMONTHS"));
    addNamedWarpScriptFunction(new ADDYEARS("ADDYEARS"));
    
    addNamedWarpScriptFunction(new QUANTIZE("QUANTIZE"));
    addNamedWarpScriptFunction(new NBOUNDS("NBOUNDS"));
    addNamedWarpScriptFunction(new LBOUNDS("LBOUNDS"));
    
    //NFIRST -> Retain at most the N first values
    //NLAST -> Retain at most the N last values
    
    //
    // GTS manipulation frameworks
    //
    
    addNamedWarpScriptFunction(new BUCKETIZE("BUCKETIZE"));
    addNamedWarpScriptFunction(new MAP("MAP"));
    addNamedWarpScriptFunction(new FILTER("FILTER", true));
    addNamedWarpScriptFunction(new APPLY("APPLY", true));
    addNamedWarpScriptFunction(new FILTER("PFILTER", false));
    addNamedWarpScriptFunction(new APPLY("PAPPLY", false));
    addNamedWarpScriptFunction(new REDUCE("REDUCE", true));
    addNamedWarpScriptFunction(new REDUCE("PREDUCE", false));
        
    addNamedWarpScriptFunction(new MaxTickSlidingWindow("max.tick.sliding.window"));
    addNamedWarpScriptFunction(new MaxTimeSlidingWindow("max.time.sliding.window"));
    addNamedWarpScriptFunction(new NULL(NULL));
    addNamedWarpScriptFunction(new ISNULL("ISNULL"));
    addNamedWarpScriptFunction(new MapperReplace.Builder("mapper.replace"));
    addNamedWarpScriptFunction(new MAPPERGT("mapper.gt"));
    addNamedWarpScriptFunction(new MAPPERGE("mapper.ge"));
    addNamedWarpScriptFunction(new MAPPEREQ("mapper.eq"));
    addNamedWarpScriptFunction(new MAPPERNE("mapper.ne"));
    addNamedWarpScriptFunction(new MAPPERLE("mapper.le"));
    addNamedWarpScriptFunction(new MAPPERLT("mapper.lt"));
    addNamedWarpScriptFunction(new MapperAdd.Builder("mapper.add"));
    addNamedWarpScriptFunction(new MapperMul.Builder("mapper.mul"));
    addNamedWarpScriptFunction(new MapperPow.Builder("mapper.pow"));
    try {
      addNamedWarpScriptFunction(new MapperPow("mapper.sqrt", 0.5D));
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
    addNamedWarpScriptFunction(new MapperExp.Builder("mapper.exp"));
    addNamedWarpScriptFunction(new MapperLog.Builder("mapper.log"));
    addNamedWarpScriptFunction(new MapperMinX.Builder("mapper.min.x"));
    addNamedWarpScriptFunction(new MapperMaxX.Builder("mapper.max.x"));
    addNamedWarpScriptFunction(new MapperParseDouble.Builder("mapper.parsedouble"));
    
    addNamedWarpScriptFunction(new MapperTick.Builder("mapper.tick"));
    addNamedWarpScriptFunction(new MapperYear.Builder("mapper.year"));
    addNamedWarpScriptFunction(new MapperMonthOfYear.Builder("mapper.month"));
    addNamedWarpScriptFunction(new MapperDayOfMonth.Builder("mapper.day"));
    addNamedWarpScriptFunction(new MapperDayOfWeek.Builder("mapper.weekday"));
    addNamedWarpScriptFunction(new MapperHourOfDay.Builder("mapper.hour"));
    addNamedWarpScriptFunction(new MapperMinuteOfHour.Builder("mapper.minute"));
    addNamedWarpScriptFunction(new MapperSecondOfMinute.Builder("mapper.second"));

    addNamedWarpScriptFunction(new MapperNPDF.Builder("mapper.npdf"));
    addNamedWarpScriptFunction(new MapperDotProduct.Builder("mapper.dotproduct"));

    addNamedWarpScriptFunction(new MapperDotProductTanh.Builder("mapper.dotproduct.tanh"));
    addNamedWarpScriptFunction(new MapperDotProductSigmoid.Builder("mapper.dotproduct.sigmoid"));
    addNamedWarpScriptFunction(new MapperDotProductPositive.Builder("mapper.dotproduct.positive"));

    // Kernel mappers
    addNamedWarpScriptFunction(new MapperKernelCosine("mapper.kernel.cosine"));
    addNamedWarpScriptFunction(new MapperKernelEpanechnikov("mapper.kernel.epanechnikov"));
    addNamedWarpScriptFunction(new MapperKernelGaussian("mapper.kernel.gaussian"));
    addNamedWarpScriptFunction(new MapperKernelLogistic("mapper.kernel.logistic"));
    addNamedWarpScriptFunction(new MapperKernelQuartic("mapper.kernel.quartic"));
    addNamedWarpScriptFunction(new MapperKernelSilverman("mapper.kernel.silverman"));
    addNamedWarpScriptFunction(new MapperKernelTriangular("mapper.kernel.triangular"));
    addNamedWarpScriptFunction(new MapperKernelTricube("mapper.kernel.tricube"));
    addNamedWarpScriptFunction(new MapperKernelTriweight("mapper.kernel.triweight"));
    addNamedWarpScriptFunction(new MapperKernelUniform("mapper.kernel.uniform"));
        
    addNamedWarpScriptFunction(new Percentile.Builder("mapper.percentile"));
    
    //functions.put("mapper.abscissa", new MapperSAX.Builder());
    
    addNamedWarpScriptFunction(new FilterByClass.Builder("filter.byclass"));
    addNamedWarpScriptFunction(new FilterByLabels.Builder("filter.bylabels", true, false));
    addNamedWarpScriptFunction(new FilterByLabels.Builder("filter.byattr", false, true));
    addNamedWarpScriptFunction(new FilterByLabels.Builder("filter.bylabelsattr", true, true));
    addNamedWarpScriptFunction(new FilterByMetadata.Builder("filter.bymetadata"));

    addNamedWarpScriptFunction(new FilterLastEQ.Builder("filter.last.eq"));
    addNamedWarpScriptFunction(new FilterLastGE.Builder("filter.last.ge"));
    addNamedWarpScriptFunction(new FilterLastGT.Builder("filter.last.gt"));
    addNamedWarpScriptFunction(new FilterLastLE.Builder("filter.last.le"));
    addNamedWarpScriptFunction(new FilterLastLT.Builder("filter.last.lt"));
    addNamedWarpScriptFunction(new FilterLastNE.Builder("filter.last.ne"));

    addNamedWarpScriptFunction(new LatencyFilter.Builder("filter.latencies"));
    
    //
    // Fillers
    //
    
    addNamedWarpScriptFunction(new FillerNext("filler.next"));
    addNamedWarpScriptFunction(new FillerPrevious("filler.previous"));
    addNamedWarpScriptFunction(new FillerInterpolate("filler.interpolate"));
    addNamedWarpScriptFunction(new FillerTrend("filler.trend"));
 
    //
    // Geo Manipulation functions
    //
    
    addNamedWarpScriptFunction(new TOHHCODE("->HHCODE", true));
    addNamedWarpScriptFunction(new TOHHCODE("->HHCODELONG", false));
    addNamedWarpScriptFunction(new HHCODETO("HHCODE->"));
    addNamedWarpScriptFunction(new GEOREGEXP("GEO.REGEXP"));
    addNamedWarpScriptFunction(new GeoWKT(GEO_WKT, false));
    addNamedWarpScriptFunction(new GeoWKT(GEO_WKT_UNIFORM, true));
    addNamedWarpScriptFunction(new GeoJSON(GEO_JSON, false));
    addNamedWarpScriptFunction(new GeoJSON(GEO_JSON_UNIFORM, true));
    addNamedWarpScriptFunction(new GEOOPTIMIZE("GEO.OPTIMIZE"));
    addNamedWarpScriptFunction(new GeoIntersection(GEO_INTERSECTION));
    addNamedWarpScriptFunction(new GeoUnion(GEO_UNION));
    addNamedWarpScriptFunction(new GeoSubtraction(GEO_DIFFERENCE));
    addNamedWarpScriptFunction(new GEOWITHIN("GEO.WITHIN"));
    addNamedWarpScriptFunction(new GEOINTERSECTS("GEO.INTERSECTS"));
    addNamedWarpScriptFunction(new HAVERSINE("HAVERSINE"));
    addNamedWarpScriptFunction(new GEOPACK(GEOPACK));
    addNamedWarpScriptFunction(new GEOUNPACK(GEOUNPACK));
    addNamedWarpScriptFunction(new MapperGeoWithin.Builder("mapper.geo.within"));
    addNamedWarpScriptFunction(new MapperGeoOutside.Builder("mapper.geo.outside"));
    addNamedWarpScriptFunction(new MapperGeoApproximate.Builder("mapper.geo.approximate"));
    addNamedWarpScriptFunction(new COPYGEO("COPYGEO"));
    addNamedWarpScriptFunction(new BBOX("BBOX"));
    addNamedWarpScriptFunction(new TOGEOHASH("->GEOHASH"));
    addNamedWarpScriptFunction(new GEOHASHTO("GEOHASH->"));
    
    //
    // Counters
    //
    
    addNamedWarpScriptFunction(new COUNTER(COUNTER));
    addNamedWarpScriptFunction(new COUNTERVALUE("COUNTERVALUE"));
    addNamedWarpScriptFunction(new COUNTERDELTA("COUNTERDELTA"));
    addNamedWarpScriptFunction(new COUNTERSET(COUNTERSET));

    //
    // Math functions
    //
    
    addNamedWarpScriptFunction(new Pi("pi"));
    addNamedWarpScriptFunction(new Pi("PI"));
    addNamedWarpScriptFunction(new E("e"));
    addNamedWarpScriptFunction(new E("E"));
    addNamedWarpScriptFunction(new MINLONG("MINLONG"));
    addNamedWarpScriptFunction(new MAXLONG("MAXLONG"));
    addNamedWarpScriptFunction(new RAND("RAND"));
    addNamedWarpScriptFunction(new PRNG("PRNG"));
    addNamedWarpScriptFunction(new SRAND("SRAND"));

    addNamedWarpScriptFunction(new NPDF.Builder("NPDF"));
    addNamedWarpScriptFunction(new MUSIGMA("MUSIGMA"));
    addNamedWarpScriptFunction(new KURTOSIS("KURTOSIS"));
    addNamedWarpScriptFunction(new SKEWNESS("SKEWNESS"));
    addNamedWarpScriptFunction(new NSUMSUMSQ("NSUMSUMSQ"));
    addNamedWarpScriptFunction(new LR("LR"));
    addNamedWarpScriptFunction(new MODE("MODE"));
    
    addNamedWarpScriptFunction(new TOZ("->Z"));
    addNamedWarpScriptFunction(new ZTO("Z->"));
    addNamedWarpScriptFunction(new PACK("PACK"));
    addNamedWarpScriptFunction(new UNPACK("UNPACK"));
    
    //
    // Linear Algebra
    //
    
    addNamedWarpScriptFunction(new TOMAT("->MAT"));
    addNamedWarpScriptFunction(new MATTO("MAT->"));
    addNamedWarpScriptFunction(new TR("TR"));
    addNamedWarpScriptFunction(new TRANSPOSE("TRANSPOSE"));
    addNamedWarpScriptFunction(new DET("DET"));
    addNamedWarpScriptFunction(new INV("INV"));
    addNamedWarpScriptFunction(new TOVEC("->VEC"));
    addNamedWarpScriptFunction(new VECTO("VEC->"));

    addNamedWarpScriptFunction(new COS("COS"));
    addNamedWarpScriptFunction(new COSH("COSH"));
    addNamedWarpScriptFunction(new ACOS("ACOS"));

    addNamedWarpScriptFunction(new SIN("SIN"));
    addNamedWarpScriptFunction(new SINH("SINH"));
    addNamedWarpScriptFunction(new ASIN("ASIN"));

    addNamedWarpScriptFunction(new TAN("TAN"));
    addNamedWarpScriptFunction(new TANH("TANH"));
    addNamedWarpScriptFunction(new ATAN("ATAN"));

    addNamedWarpScriptFunction(new SIGNUM("SIGNUM"));
    addNamedWarpScriptFunction(new FLOOR("FLOOR"));
    addNamedWarpScriptFunction(new CEIL("CEIL"));
    addNamedWarpScriptFunction(new ROUND("ROUND"));

    addNamedWarpScriptFunction(new RINT("RINT"));
    addNamedWarpScriptFunction(new NEXTUP("NEXTUP"));
    addNamedWarpScriptFunction(new ULP("ULP"));

    addNamedWarpScriptFunction(new SQRT("SQRT"));
    addNamedWarpScriptFunction(new CBRT("CBRT"));
    addNamedWarpScriptFunction(new EXP("EXP"));
    addNamedWarpScriptFunction(new EXPM1("EXPM1"));
    addNamedWarpScriptFunction(new LOG("LOG"));
    addNamedWarpScriptFunction(new LOG10("LOG10"));
    addNamedWarpScriptFunction(new LOG1P("LOG1P"));

    addNamedWarpScriptFunction(new TORADIANS("TORADIANS"));
    addNamedWarpScriptFunction(new TODEGREES("TODEGREES"));

    addNamedWarpScriptFunction(new MAX("MAX"));
    addNamedWarpScriptFunction(new MIN("MIN"));

    addNamedWarpScriptFunction(new COPYSIGN("COPYSIGN"));
    addNamedWarpScriptFunction(new HYPOT("HYPOT"));
    addNamedWarpScriptFunction(new IEEEREMAINDER("IEEEREMAINDER"));
    addNamedWarpScriptFunction(new NEXTAFTER("NEXTAFTER"));
    addNamedWarpScriptFunction(new ATAN2("ATAN2"));

    addNamedWarpScriptFunction(new FLOORDIV("FLOORDIV"));
    addNamedWarpScriptFunction(new FLOORMOD("FLOORMOD"));

    addNamedWarpScriptFunction(new ADDEXACT("ADDEXACT"));
    addNamedWarpScriptFunction(new SUBTRACTEXACT("SUBTRACTEXACT"));
    addNamedWarpScriptFunction(new MULTIPLYEXACT("MULTIPLYEXACT"));
    addNamedWarpScriptFunction(new INCREMENTEXACT("INCREMENTEXACT"));
    addNamedWarpScriptFunction(new DECREMENTEXACT("DECREMENTEXACT"));
    addNamedWarpScriptFunction(new NEGATEEXACT("NEGATEEXACT"));
    addNamedWarpScriptFunction(new TOINTEXACT("TOINTEXACT"));

    addNamedWarpScriptFunction(new SCALB("SCALB"));
    addNamedWarpScriptFunction(new RANDOM("RANDOM"));
    addNamedWarpScriptFunction(new NEXTDOWN("NEXTDOWN"));
    addNamedWarpScriptFunction(new GETEXPONENT("GETEXPONENT"));
    
    addNamedWarpScriptFunction(new IDENT("IDENT"));
    
    //
    // Processing
    //

    addNamedWarpScriptFunction(new Pencode("Pencode"));

    // Structure
    
    addNamedWarpScriptFunction(new PpushStyle("PpushStyle"));
    addNamedWarpScriptFunction(new PpopStyle("PpopStyle"));

    // Environment
    
    
    // Shape
    
    addNamedWarpScriptFunction(new Parc("Parc"));
    addNamedWarpScriptFunction(new Pellipse("Pellipse"));
    addNamedWarpScriptFunction(new Ppoint("Ppoint"));
    addNamedWarpScriptFunction(new Pline("Pline"));
    addNamedWarpScriptFunction(new Ptriangle("Ptriangle"));
    addNamedWarpScriptFunction(new Prect("Prect"));
    addNamedWarpScriptFunction(new Pquad("Pquad"));
    
    addNamedWarpScriptFunction(new Pbezier("Pbezier"));
    addNamedWarpScriptFunction(new PbezierPoint("PbezierPoint"));
    addNamedWarpScriptFunction(new PbezierTangent("PbezierTangent"));
    addNamedWarpScriptFunction(new PbezierDetail("PbezierDetail"));
    
    addNamedWarpScriptFunction(new Pcurve("Pcurve"));
    addNamedWarpScriptFunction(new PcurvePoint("PcurvePoint"));
    addNamedWarpScriptFunction(new PcurveTangent("PcurveTangent"));
    addNamedWarpScriptFunction(new PcurveDetail("PcurveDetail"));
    addNamedWarpScriptFunction(new PcurveTightness("PcurveTightness"));

    addNamedWarpScriptFunction(new Pbox("Pbox"));
    addNamedWarpScriptFunction(new Psphere("Psphere"));
    addNamedWarpScriptFunction(new PsphereDetail("PsphereDetail"));
    
    addNamedWarpScriptFunction(new PellipseMode("PellipseMode"));
    addNamedWarpScriptFunction(new PrectMode("PrectMode"));
    addNamedWarpScriptFunction(new PstrokeCap("PstrokeCap"));
    addNamedWarpScriptFunction(new PstrokeJoin("PstrokeJoin"));
    addNamedWarpScriptFunction(new PstrokeWeight("PstrokeWeight"));
    
    addNamedWarpScriptFunction(new PbeginShape("PbeginShape"));
    addNamedWarpScriptFunction(new PendShape("PendShape"));
    addNamedWarpScriptFunction(new PloadShape("PloadShape"));
    addNamedWarpScriptFunction(new PbeginContour("PbeginContour"));
    addNamedWarpScriptFunction(new PendContour("PendContour"));
    addNamedWarpScriptFunction(new Pvertex("Pvertex"));
    addNamedWarpScriptFunction(new PcurveVertex("PcurveVertex"));
    addNamedWarpScriptFunction(new PbezierVertex("PbezierVertex"));
    addNamedWarpScriptFunction(new PquadraticVertex("PquadraticVertex"));
    
    // TODO(hbs): support PShape (need to support PbeginShape etc applied to PShape instances)
    addNamedWarpScriptFunction(new PshapeMode("PshapeMode"));
    addNamedWarpScriptFunction(new Pshape("Pshape"));
    
    // Transform
    
    addNamedWarpScriptFunction(new PpushMatrix("PpushMatrix"));
    addNamedWarpScriptFunction(new PpopMatrix("PpopMatrix"));
    addNamedWarpScriptFunction(new PresetMatrix("PresetMatrix"));
    addNamedWarpScriptFunction(new Protate("Protate"));
    addNamedWarpScriptFunction(new ProtateX("ProtateX"));
    addNamedWarpScriptFunction(new ProtateY("ProtateY"));
    addNamedWarpScriptFunction(new ProtateZ("ProtateZ"));
    addNamedWarpScriptFunction(new Pscale("Pscale"));
    addNamedWarpScriptFunction(new PshearX("PshearX"));
    addNamedWarpScriptFunction(new PshearY("PshearY"));
    addNamedWarpScriptFunction(new Ptranslate("Ptranslate"));
    
    // Color
    
    addNamedWarpScriptFunction(new Pbackground("Pbackground"));
    addNamedWarpScriptFunction(new PcolorMode("PcolorMode"));
    addNamedWarpScriptFunction(new Pclear("Pclear"));
    addNamedWarpScriptFunction(new Pfill("Pfill"));
    addNamedWarpScriptFunction(new PnoFill("PnoFill"));
    addNamedWarpScriptFunction(new Pstroke("Pstroke"));
    addNamedWarpScriptFunction(new PnoStroke("PnoStroke"));
    
    addNamedWarpScriptFunction(new Palpha("Palpha"));
    addNamedWarpScriptFunction(new Pblue("Pblue"));
    addNamedWarpScriptFunction(new Pbrightness("Pbrightness"));
    addNamedWarpScriptFunction(new Pcolor("Pcolor"));
    addNamedWarpScriptFunction(new Pgreen("Pgreen"));
    addNamedWarpScriptFunction(new Phue("Phue"));
    addNamedWarpScriptFunction(new PlerpColor("PlerpColor"));
    addNamedWarpScriptFunction(new Pred("Pred"));
    addNamedWarpScriptFunction(new Psaturation("Psaturation"));
    
    // Image
    
    addNamedWarpScriptFunction(new Pdecode("Pdecode"));
    addNamedWarpScriptFunction(new Pimage("Pimage"));
    addNamedWarpScriptFunction(new PimageMode("PimageMode"));
    addNamedWarpScriptFunction(new Ptint("Ptint"));
    addNamedWarpScriptFunction(new PnoTint("PnoTint"));
    addNamedWarpScriptFunction(new Ppixels("Ppixels"));
    addNamedWarpScriptFunction(new PupdatePixels("PupdatePixels"));
    addNamedWarpScriptFunction(new PtoImage("PtoImage"));
    
    // TODO(hbs): support texture related functions?
    
    addNamedWarpScriptFunction(new Pblend("Pblend"));
    addNamedWarpScriptFunction(new Pcopy("Pcopy"));
    addNamedWarpScriptFunction(new Pget("Pget"));
    addNamedWarpScriptFunction(new Pset("Pset"));
    addNamedWarpScriptFunction(new Pfilter("Pfilter"));

    // Rendering
    
    addNamedWarpScriptFunction(new PblendMode("PblendMode"));
    addNamedWarpScriptFunction(new Pclip("Pclip"));
    addNamedWarpScriptFunction(new PnoClip("PnoClip"));
    addNamedWarpScriptFunction(new PGraphics("PGraphics"));

    // TODO(hbs): support shaders?
    
    // Typography
    
    addNamedWarpScriptFunction(new PcreateFont("PcreateFont"));
    addNamedWarpScriptFunction(new Ptext("Ptext"));
    addNamedWarpScriptFunction(new PtextAlign("PtextAlign"));
    addNamedWarpScriptFunction(new PtextAscent("PtextAscent"));
    addNamedWarpScriptFunction(new PtextDescent("PtextDescent"));
    addNamedWarpScriptFunction(new PtextFont("PtextFont"));
    addNamedWarpScriptFunction(new PtextLeading("PtextLeading"));
    addNamedWarpScriptFunction(new PtextMode("PtextMode"));
    addNamedWarpScriptFunction(new PtextSize("PtextSize"));
    addNamedWarpScriptFunction(new PtextWidth("PtextWidth"));
    
    // Math
    
    addNamedWarpScriptFunction(new Pconstrain("Pconstrain"));
    addNamedWarpScriptFunction(new Pdist("Pdist"));
    addNamedWarpScriptFunction(new Plerp("Plerp"));
    addNamedWarpScriptFunction(new Pmag("Pmag"));
    addNamedWarpScriptFunction(new Pmap("Pmap"));
    addNamedWarpScriptFunction(new Pnorm("Pnorm"));
    
    ////////////////////////////////////////////////////////////////////////////
    
    //
    // Moved from JavaLibrary
    //
    /////////////////////////
    
    //
    // Bucketizers
    //

    addNamedWarpScriptFunction(new And("bucketizer.and", false));
    addNamedWarpScriptFunction(new First("bucketizer.first"));
    addNamedWarpScriptFunction(new Last("bucketizer.last"));
    addNamedWarpScriptFunction(new Min("bucketizer.min", true));
    addNamedWarpScriptFunction(new Max("bucketizer.max", true));
    addNamedWarpScriptFunction(new Mean("bucketizer.mean", false));
    addNamedWarpScriptFunction(new Median("bucketizer.median"));
    addNamedWarpScriptFunction(new MAD("bucketizer.mad"));
    addNamedWarpScriptFunction(new Or("bucketizer.or", false));
    addNamedWarpScriptFunction(new Sum("bucketizer.sum", true));
    addNamedWarpScriptFunction(new Join.Builder("bucketizer.join", true, false, null));
    addNamedWarpScriptFunction(new Count("bucketizer.count", false));
    addNamedWarpScriptFunction(new Percentile.Builder("bucketizer.percentile"));
    addNamedWarpScriptFunction(new Min("bucketizer.min.forbid-nulls", false));
    addNamedWarpScriptFunction(new Max("bucketizer.max.forbid-nulls", false));
    addNamedWarpScriptFunction(new Mean("bucketizer.mean.exclude-nulls", true));
    addNamedWarpScriptFunction(new Sum("bucketizer.sum.forbid-nulls", false));
    addNamedWarpScriptFunction(new Join.Builder("bucketizer.join.forbid-nulls", false, false, null));
    addNamedWarpScriptFunction(new Count("bucketizer.count.exclude-nulls", true));
    addNamedWarpScriptFunction(new Count("bucketizer.count.include-nulls", false));
    addNamedWarpScriptFunction(new Count("bucketizer.count.nonnull", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("bucketizer.mean.circular", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("bucketizer.mean.circular.exclude-nulls", false));
    addNamedWarpScriptFunction(new RMS("bucketizer.rms", false));
    //
    // Mappers
    //

    addNamedWarpScriptFunction(new And("mapper.and", false));
    addNamedWarpScriptFunction(new Count("mapper.count", false));
    addNamedWarpScriptFunction(new First("mapper.first"));
    addNamedWarpScriptFunction(new Last("mapper.last"));
    addNamedWarpScriptFunction(new Min(MAPPER_MIN, true));
    addNamedWarpScriptFunction(new Max(MAPPER_MAX, true));
    addNamedWarpScriptFunction(new Mean("mapper.mean", false));
    addNamedWarpScriptFunction(new Median("mapper.median"));
    addNamedWarpScriptFunction(new MAD("mapper.mad"));
    addNamedWarpScriptFunction(new Or("mapper.or", false));
    addNamedWarpScriptFunction(new Highest(MAPPER_HIGHEST));
    addNamedWarpScriptFunction(new Lowest(MAPPER_LOWEST));
    addNamedWarpScriptFunction(new Sum("mapper.sum", true));
    addNamedWarpScriptFunction(new Join.Builder("mapper.join", true, false, null));
    addNamedWarpScriptFunction(new Delta("mapper.delta"));
    addNamedWarpScriptFunction(new Rate("mapper.rate"));
    addNamedWarpScriptFunction(new HSpeed("mapper.hspeed"));
    addNamedWarpScriptFunction(new HDist("mapper.hdist"));
    addNamedWarpScriptFunction(new TrueCourse("mapper.truecourse"));
    addNamedWarpScriptFunction(new VSpeed("mapper.vspeed"));
    addNamedWarpScriptFunction(new VDist("mapper.vdist"));
    addNamedWarpScriptFunction(new Variance.Builder("mapper.var", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("mapper.sd", false));
    addNamedWarpScriptFunction(new MapperAbs("mapper.abs"));
    addNamedWarpScriptFunction(new MapperCeil("mapper.ceil"));
    addNamedWarpScriptFunction(new MapperFloor("mapper.floor"));
    addNamedWarpScriptFunction(new MapperFinite("mapper.finite"));
    addNamedWarpScriptFunction(new MapperRound("mapper.round"));
    addNamedWarpScriptFunction(new MapperToBoolean("mapper.toboolean"));
    addNamedWarpScriptFunction(new MapperToLong("mapper.tolong"));
    addNamedWarpScriptFunction(new MapperToDouble("mapper.todouble"));
    addNamedWarpScriptFunction(new MapperToString("mapper.tostring"));
    addNamedWarpScriptFunction(new MapperTanh("mapper.tanh"));
    addNamedWarpScriptFunction(new MapperSigmoid("mapper.sigmoid"));
    addNamedWarpScriptFunction(new MapperProduct("mapper.product"));
    addNamedWarpScriptFunction(new MapperGeoClearPosition("mapper.geo.clear"));
    addNamedWarpScriptFunction(new Count("mapper.count.exclude-nulls", true));
    addNamedWarpScriptFunction(new Count("mapper.count.include-nulls", false));
    addNamedWarpScriptFunction(new Count("mapper.count.nonnull", true));
    addNamedWarpScriptFunction(new Min("mapper.min.forbid-nulls", false));
    addNamedWarpScriptFunction(new Max("mapper.max.forbid-nulls", false));
    addNamedWarpScriptFunction(new Mean("mapper.mean.exclude-nulls", true));
    addNamedWarpScriptFunction(new Sum("mapper.sum.forbid-nulls", false));
    addNamedWarpScriptFunction(new Join.Builder("mapper.join.forbid-nulls", false, false, null));
    addNamedWarpScriptFunction(new Variance.Builder("mapper.var.forbid-nulls", true));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("mapper.sd.forbid-nulls", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("mapper.mean.circular", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("mapper.mean.circular.exclude-nulls", false));
    addNamedWarpScriptFunction(new MapperMod.Builder("mapper.mod"));
    addNamedWarpScriptFunction(new RMS("mapper.rms", false));

    //
    // Reducers
    //

    addNamedWarpScriptFunction(new And("reducer.and", false));
    addNamedWarpScriptFunction(new And("reducer.and.exclude-nulls", true));
    addNamedWarpScriptFunction(new Min("reducer.min", true));
    addNamedWarpScriptFunction(new Min("reducer.min.forbid-nulls", false));
    addNamedWarpScriptFunction(new Min("reducer.min.nonnull", false));
    addNamedWarpScriptFunction(new Max("reducer.max", true));
    addNamedWarpScriptFunction(new Max("reducer.max.forbid-nulls", false));
    addNamedWarpScriptFunction(new Max("reducer.max.nonnull", false));
    addNamedWarpScriptFunction(new Mean("reducer.mean", false));
    addNamedWarpScriptFunction(new Mean("reducer.mean.exclude-nulls", true));
    addNamedWarpScriptFunction(new Median("reducer.median"));
    addNamedWarpScriptFunction(new MAD("reducer.mad"));
    addNamedWarpScriptFunction(new Or("reducer.or", false));
    addNamedWarpScriptFunction(new Or("reducer.or.exclude-nulls", true));
    addNamedWarpScriptFunction(new Sum("reducer.sum", true));
    addNamedWarpScriptFunction(new Sum("reducer.sum.forbid-nulls", false));
    addNamedWarpScriptFunction(new Sum("reducer.sum.nonnull", false));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join", true, false, null));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join.forbid-nulls", false, false, null));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join.nonnull", false, false, null));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join.urlencoded", false, true, ""));
    addNamedWarpScriptFunction(new Variance.Builder("reducer.var", false));
    addNamedWarpScriptFunction(new Variance.Builder("reducer.var.forbid-nulls", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("reducer.sd", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("reducer.sd.forbid-nulls", false));
    addNamedWarpScriptFunction(new Argminmax.Builder("reducer.argmin", true));
    addNamedWarpScriptFunction(new Argminmax.Builder("reducer.argmax", false));
    addNamedWarpScriptFunction(new MapperProduct("reducer.product"));
    addNamedWarpScriptFunction(new Count("reducer.count", false));
    addNamedWarpScriptFunction(new Count("reducer.count.include-nulls", false));
    addNamedWarpScriptFunction(new Count("reducer.count.exclude-nulls", true));
    addNamedWarpScriptFunction(new Count("reducer.count.nonnull", true));
    addNamedWarpScriptFunction(new ShannonEntropy("reducer.shannonentropy.0", false));
    addNamedWarpScriptFunction(new ShannonEntropy("reducer.shannonentropy.1", true));
    addNamedWarpScriptFunction(new Percentile.Builder("reducer.percentile"));
    addNamedWarpScriptFunction(new CircularMean.Builder("reducer.mean.circular", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("reducer.mean.circular.exclude-nulls", false));
    addNamedWarpScriptFunction(new RMS("reducer.rms", false));
    addNamedWarpScriptFunction(new RMS("reducer.rms.exclude-nulls", true));

    //
    // Filters
    //
    
    //
    // N-ary ops
    //
    
    addNamedWarpScriptFunction(new OpAdd("op.add", true));
    addNamedWarpScriptFunction(new OpAdd("op.add.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpSub("op.sub"));
    addNamedWarpScriptFunction(new OpMul("op.mul", true));
    addNamedWarpScriptFunction(new OpMul("op.mul.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpDiv("op.div"));
    addNamedWarpScriptFunction(new OpMask("op.mask", false));
    addNamedWarpScriptFunction(new OpMask("op.negmask", true));
    addNamedWarpScriptFunction(new OpNE("op.ne"));
    addNamedWarpScriptFunction(new OpEQ("op.eq"));
    addNamedWarpScriptFunction(new OpLT("op.lt"));
    addNamedWarpScriptFunction(new OpGT("op.gt"));
    addNamedWarpScriptFunction(new OpLE("op.le"));
    addNamedWarpScriptFunction(new OpGE("op.ge"));
    addNamedWarpScriptFunction(new OpAND("op.and.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpAND("op.and", true));
    addNamedWarpScriptFunction(new OpOR("op.or.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpOR("op.or", true));

    /////////////////////////

    int nregs = Integer.parseInt(WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_REGISTERS, String.valueOf(WarpScriptStack.DEFAULT_REGISTERS)));

    addNamedWarpScriptFunction(new CLEARREGS(CLEARREGS));
    addNamedWarpScriptFunction(new VARS("VARS"));
    addNamedWarpScriptFunction(new ASREGS("ASREGS"));
    for (int i = 0; i < nregs; i++) {
      addNamedWarpScriptFunction(new POPR(POPR + i, i));
      addNamedWarpScriptFunction(new POPR(CPOPR + i, i, true));
      addNamedWarpScriptFunction(new PUSHR(PUSHR + i, i));
    }
  }

  public static void addNamedWarpScriptFunction(NamedWarpScriptFunction namedFunction) {
    functions.put(namedFunction.getName(), namedFunction);
  }
  
  public static Object getFunction(String name) {
    return functions.get(name);
  }
  
  public static void registerExtensions() { 
    Properties props = WarpConfig.getProperties();
    
    if (null == props) {
      return;
    }
    
    //
    // Extract the list of extensions
    //
    
    Set<String> ext = new LinkedHashSet<String>();
    
    if (props.containsKey(Configuration.CONFIG_WARPSCRIPT_EXTENSIONS)) {
      String[] extensions = props.getProperty(Configuration.CONFIG_WARPSCRIPT_EXTENSIONS).split(",");
      
      for (String extension: extensions) {
        ext.add(extension.trim());
      }
    }
    
    for (Object key: props.keySet()) {
      if (!key.toString().startsWith(Configuration.CONFIG_WARPSCRIPT_EXTENSION_PREFIX)) {
        continue;
      }
      
      ext.add(props.get(key).toString().trim());
    }
    
    // Sort the extensions
    List<String> sortedext = new ArrayList<String>(ext);
    sortedext.sort(null);
    
    List<String> failedExt = new ArrayList<String>();
      
    //
    // Determine the possible jar from which WarpScriptLib was loaded
    //
      
    String wsljar = null;
    URL wslurl = WarpScriptLib.class.getResource('/' + WarpScriptLib.class.getCanonicalName().replace('.',  '/') + ".class");
    if (null != wslurl && "jar".equals(wslurl.getProtocol())) {
      wsljar = wslurl.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");
    }
      
    for (String extension: sortedext) {
      
      // If the extension name contains '#', remove everything up to the last '#', this was used as a sorting prefix
            
      if (extension.contains("#")) {
        extension = extension.replaceAll("^.*#", "");
      }
      
      try {
        //
        // Locate the class using the current class loader
        //
        
        URL url = WarpScriptLib.class.getResource('/' + extension.replace('.', '/') + ".class");
        
        if (null == url) {
          LOG.error("Unable to load extension '" + extension + "', make sure it is in the class path.");
          failedExt.add(extension);
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
          if (!jarfile.equals(wsljar) && !"true".equals(props.getProperty(Configuration.CONFIG_WARPSCRIPT_DEFAULTCL_PREFIX + extension))) {
            cl = new WarpClassLoader(jarfile, WarpScriptLib.class.getClassLoader());
          }
        
          cls = Class.forName(extension, true, cl);
        } else {
          cls = Class.forName(extension, true, WarpScriptLib.class.getClassLoader());
        }

        //Class cls = Class.forName(extension);
        WarpScriptExtension wse = (WarpScriptExtension) cls.newInstance();          
        wse.register();
        
        String namespace = props.getProperty(Configuration.CONFIG_WARPSCRIPT_NAMESPACE_PREFIX + wse.getClass().getName(), "").trim(); 
        if (null != namespace && !"".equals(namespace)) {
          if (namespace.contains("%")) {
            namespace = URLDecoder.decode(namespace, "UTF-8");
          }
          LOG.info("LOADED extension '" + extension + "'" + " under namespace '" + namespace + "'.");
        } else {
          LOG.info("LOADED extension '" + extension + "'");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    if (!failedExt.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("The following WarpScript extensions could not be loaded, aborting:");
      for (String extension: failedExt) {
        sb.append(" '");
        sb.append(extension);
        sb.append("'");
      }
      LOG.error(sb.toString());
      throw new RuntimeException(sb.toString());
    }
  }
  
  public static void register(WarpScriptExtension extension) {
    String namespace = WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_NAMESPACE_PREFIX + extension.getClass().getName(), "").trim();
        
    if (namespace.contains("%")) {
      try {
        namespace = URLDecoder.decode(namespace, "UTF-8");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    register(namespace, extension);
  }
  
  public static void register(String namespace, WarpScriptExtension extension) {
    
    extloaded.add(extension.getClass().getCanonicalName());
    
    Map<String,Object> extfuncs = extension.getFunctions();
    
    if (null == extfuncs) {
      return;
    }
    
    for (Entry<String,Object> entry: extfuncs.entrySet()) {
      if (null == entry.getValue()) {
        functions.remove(namespace + entry.getKey());
      } else {
        functions.put(namespace + entry.getKey(), entry.getValue());
      }
    }          
  }
  
  public static boolean extloaded(String name) {
    return extloaded.contains(name);
  }
  
  public static List<String> extensions() {
    return new ArrayList<String>(extloaded);
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

  public static ArrayList getFunctionNames() {

    List<Object> list = new ArrayList<Object>();

    list.addAll(functions.keySet());

    return (ArrayList)list;

  }
}
