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
import io.warp10.script.functions.ADDDAYS;
import io.warp10.script.functions.ADDMONTHS;
import io.warp10.script.functions.ADDVALUE;
import io.warp10.script.functions.ADDYEARS;
import io.warp10.script.functions.AESUNWRAP;
import io.warp10.script.functions.AESWRAP;
import io.warp10.script.functions.AGO;
import io.warp10.script.functions.ALMOSTEQ;
import io.warp10.script.functions.APPEND;
import io.warp10.script.functions.APPLY;
import io.warp10.script.functions.ASSERT;
import io.warp10.script.functions.ASSERTMSG;
import io.warp10.script.functions.ATBUCKET;
import io.warp10.script.functions.ATINDEX;
import io.warp10.script.functions.ATTICK;
import io.warp10.script.functions.ATTRIBUTES;
import io.warp10.script.functions.AUTHENTICATE;
import io.warp10.script.functions.B64TO;
import io.warp10.script.functions.B64TOHEX;
import io.warp10.script.functions.B64URLTO;
import io.warp10.script.functions.BBOX;
import io.warp10.script.functions.BINTO;
import io.warp10.script.functions.BINTOHEX;
import io.warp10.script.functions.BITCOUNT;
import io.warp10.script.functions.BITGET;
import io.warp10.script.functions.BITSTOBYTES;
import io.warp10.script.functions.BREAK;
import io.warp10.script.functions.BUCKETCOUNT;
import io.warp10.script.functions.BUCKETIZE;
import io.warp10.script.functions.BUCKETSPAN;
import io.warp10.script.functions.BYTESTO;
import io.warp10.script.functions.BYTESTOBITS;
import io.warp10.script.functions.CALL;
import io.warp10.script.functions.CHUNK;
import io.warp10.script.functions.CHUNKENCODER;
import io.warp10.script.functions.CLEAR;
import io.warp10.script.functions.CLEARDEFS;
import io.warp10.script.functions.CLEARSYMBOLS;
import io.warp10.script.functions.CLEARTOMARK;
import io.warp10.script.functions.CLIP;
import io.warp10.script.functions.CLONE;
import io.warp10.script.functions.CLONEEMPTY;
import io.warp10.script.functions.COMMONTICKS;
import io.warp10.script.functions.COMPACT;
import io.warp10.script.functions.CONTAINS;
import io.warp10.script.functions.CONTAINSKEY;
import io.warp10.script.functions.CONTAINSVALUE;
import io.warp10.script.functions.CONTINUE;
import io.warp10.script.functions.COPYGEO;
import io.warp10.script.functions.COUNTER;
import io.warp10.script.functions.COUNTERDELTA;
import io.warp10.script.functions.COUNTERSET;
import io.warp10.script.functions.COUNTERVALUE;
import io.warp10.script.functions.COUNTTOMARK;
import io.warp10.script.functions.CPROB;
import io.warp10.script.functions.CROP;
import io.warp10.script.functions.CSTORE;
import io.warp10.script.functions.DEBUGOFF;
import io.warp10.script.functions.DEBUGON;
import io.warp10.script.functions.DEDUP;
import io.warp10.script.functions.DEF;
import io.warp10.script.functions.DEFINED;
import io.warp10.script.functions.DEFINEDMACRO;
import io.warp10.script.functions.DELETE;
import io.warp10.script.functions.DEPTH;
import io.warp10.script.functions.DET;
import io.warp10.script.functions.DIFFERENCE;
import io.warp10.script.functions.DIGEST;
import io.warp10.script.functions.DOC;
import io.warp10.script.functions.DOCMODE;
import io.warp10.script.functions.DOUBLEEXPONENTIALSMOOTHING;
import io.warp10.script.functions.DROP;
import io.warp10.script.functions.DROPN;
import io.warp10.script.functions.DTW;
import io.warp10.script.functions.DUP;
import io.warp10.script.functions.DUPN;
import io.warp10.script.functions.DURATION;
import io.warp10.script.functions.DWTSPLIT;
import io.warp10.script.functions.E;
import io.warp10.script.functions.ELAPSED;
import io.warp10.script.functions.ELEVATIONS;
import io.warp10.script.functions.EMPTY;
import io.warp10.script.functions.EMPTYLIST;
import io.warp10.script.functions.EMPTYMAP;
import io.warp10.script.functions.EMPTYSET;
import io.warp10.script.functions.EMPTYVECTOR;
import io.warp10.script.functions.ENCODERTO;
import io.warp10.script.functions.ENDLIST;
import io.warp10.script.functions.ENDMAP;
import io.warp10.script.functions.ENDSET;
import io.warp10.script.functions.ENDVECTOR;
import io.warp10.script.functions.ERROR;
import io.warp10.script.functions.ESDTEST;
import io.warp10.script.functions.EVAL;
import io.warp10.script.functions.EVALSECURE;
import io.warp10.script.functions.EVERY;
import io.warp10.script.functions.EXPORT;
import io.warp10.script.functions.EXTLOADED;
import io.warp10.script.functions.FAIL;
import io.warp10.script.functions.FDWT;
import io.warp10.script.functions.FETCH;
import io.warp10.script.functions.FFTWINDOW;
import io.warp10.script.functions.FILLNEXT;
import io.warp10.script.functions.FILLPREVIOUS;
import io.warp10.script.functions.FILLTICKS;
import io.warp10.script.functions.FILLVALUE;
import io.warp10.script.functions.FILTER;
import io.warp10.script.functions.FILTERBY;
import io.warp10.script.functions.FIND;
import io.warp10.script.functions.FINDSTATS;
import io.warp10.script.functions.FIRSTTICK;
import io.warp10.script.functions.FLATTEN;
import io.warp10.script.functions.FOR;
import io.warp10.script.functions.FOREACH;
import io.warp10.script.functions.FORGET;
import io.warp10.script.functions.FORSTEP;
import io.warp10.script.functions.FROMTSELEMENTS;
import io.warp10.script.functions.FUSE;
import io.warp10.script.functions.GEOHASHTO;
import io.warp10.script.functions.GEOINTERSECTS;
import io.warp10.script.functions.GEOOPTIMIZE;
import io.warp10.script.functions.GEOPACK;
import io.warp10.script.functions.GEOREGEXP;
import io.warp10.script.functions.GEOUNPACK;
import io.warp10.script.functions.GEOWITHIN;
import io.warp10.script.functions.GET;
import io.warp10.script.functions.GETHOOK;
import io.warp10.script.functions.GETSECTION;
import io.warp10.script.functions.GROUPBY;
import io.warp10.script.functions.GRUBBSTEST;
import io.warp10.script.functions.GZIP;
import io.warp10.script.functions.GeoIntersection;
import io.warp10.script.functions.GeoJSON;
import io.warp10.script.functions.GeoSubtraction;
import io.warp10.script.functions.GeoUnion;
import io.warp10.script.functions.GeoWKT;
import io.warp10.script.functions.HASH;
import io.warp10.script.functions.HAVERSINE;
import io.warp10.script.functions.HEADER;
import io.warp10.script.functions.HEXTO;
import io.warp10.script.functions.HEXTOB64;
import io.warp10.script.functions.HEXTOBIN;
import io.warp10.script.functions.HHCODETO;
import io.warp10.script.functions.HMAC;
import io.warp10.script.functions.HUMANDURATION;
import io.warp10.script.functions.HYBRIDTEST;
import io.warp10.script.functions.HYBRIDTEST2;
import io.warp10.script.functions.IDENT;
import io.warp10.script.functions.IDWT;
import io.warp10.script.functions.IFT;
import io.warp10.script.functions.IFTE;
import io.warp10.script.functions.IMMUTABLE;
import io.warp10.script.functions.INFO;
import io.warp10.script.functions.INFOMODE;
import io.warp10.script.functions.INTEGRATE;
import io.warp10.script.functions.INTERSECTION;
import io.warp10.script.functions.INV;
import io.warp10.script.functions.ISAUTHENTICATED;
import io.warp10.script.functions.ISNULL;
import io.warp10.script.functions.ISNaN;
import io.warp10.script.functions.ISO8601;
import io.warp10.script.functions.ISODURATION;
import io.warp10.script.functions.ISONORMALIZE;
import io.warp10.script.functions.JOIN;
import io.warp10.script.functions.JSONLOOSE;
import io.warp10.script.functions.JSONSTRICT;
import io.warp10.script.functions.JSONTO;
import io.warp10.script.functions.KEYLIST;
import io.warp10.script.functions.KURTOSIS;
import io.warp10.script.functions.LABELS;
import io.warp10.script.functions.LASTBUCKET;
import io.warp10.script.functions.LASTSORT;
import io.warp10.script.functions.LASTTICK;
import io.warp10.script.functions.LBOUNDS;
import io.warp10.script.functions.LFLATMAP;
import io.warp10.script.functions.LIMIT;
import io.warp10.script.functions.LINEOFF;
import io.warp10.script.functions.LINEON;
import io.warp10.script.functions.LISTTO;
import io.warp10.script.functions.LMAP;
import io.warp10.script.functions.LOAD;
import io.warp10.script.functions.LOCATIONS;
import io.warp10.script.functions.LOCSTRINGS;
import io.warp10.script.functions.LOWESS;
import io.warp10.script.functions.LR;
import io.warp10.script.functions.LSORT;
import io.warp10.script.functions.LTTB;
import io.warp10.script.functions.MACROFILTER;
import io.warp10.script.functions.MACROMAPPER;
import io.warp10.script.functions.MACROTTL;
import io.warp10.script.functions.MAKEGTS;
import io.warp10.script.functions.MAN;
import io.warp10.script.functions.MAP;
import io.warp10.script.functions.MAPID;
import io.warp10.script.functions.MAPPEREQ;
import io.warp10.script.functions.MAPPERGE;
import io.warp10.script.functions.MAPPERGT;
import io.warp10.script.functions.MAPPERLE;
import io.warp10.script.functions.MAPPERLT;
import io.warp10.script.functions.MAPPERNE;
import io.warp10.script.functions.MAPTO;
import io.warp10.script.functions.MARK;
import io.warp10.script.functions.MATCH;
import io.warp10.script.functions.MATCHER;
import io.warp10.script.functions.MATTO;
import io.warp10.script.functions.MAXBUCKETS;
import io.warp10.script.functions.MAXDEPTH;
import io.warp10.script.functions.MAXGEOCELLS;
import io.warp10.script.functions.MAXGTS;
import io.warp10.script.functions.MAXLONG;
import io.warp10.script.functions.MAXLOOP;
import io.warp10.script.functions.MAXOPS;
import io.warp10.script.functions.MAXPIXELS;
import io.warp10.script.functions.MAXRECURSION;
import io.warp10.script.functions.MAXSYMBOLS;
import io.warp10.script.functions.MERGE;
import io.warp10.script.functions.META;
import io.warp10.script.functions.METASORT;
import io.warp10.script.functions.MINLONG;
import io.warp10.script.functions.MINREV;
import io.warp10.script.functions.MODE;
import io.warp10.script.functions.MONOTONIC;
import io.warp10.script.functions.MSGFAIL;
import io.warp10.script.functions.MSORT;
import io.warp10.script.functions.MSTU;
import io.warp10.script.functions.MUSIGMA;
import io.warp10.script.functions.MaxTickSlidingWindow;
import io.warp10.script.functions.MaxTimeSlidingWindow;
import io.warp10.script.functions.NAME;
import io.warp10.script.functions.NBOUNDS;
import io.warp10.script.functions.NDEBUGON;
import io.warp10.script.functions.NEWENCODER;
import io.warp10.script.functions.NEWGTS;
import io.warp10.script.functions.NONEMPTY;
import io.warp10.script.functions.NONNULL;
import io.warp10.script.functions.NOOP;
import io.warp10.script.functions.NORMALIZE;
import io.warp10.script.functions.NOTAFTER;
import io.warp10.script.functions.NOTBEFORE;
import io.warp10.script.functions.NOTIMINGS;
import io.warp10.script.functions.NOW;
import io.warp10.script.functions.NPDF;
import io.warp10.script.functions.NRETURN;
import io.warp10.script.functions.NSUMSUMSQ;
import io.warp10.script.functions.NULL;
import io.warp10.script.functions.NaN;
import io.warp10.script.functions.ONLYBUCKETS;
import io.warp10.script.functions.OPB64TO;
import io.warp10.script.functions.OPB64TOHEX;
import io.warp10.script.functions.OPS;
import io.warp10.script.functions.OPTDTW;
import io.warp10.script.functions.OPTIMIZE;
import io.warp10.script.functions.PACK;
import io.warp10.script.functions.PARSE;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.script.functions.PARTITION;
import io.warp10.script.functions.PATTERNDETECTION;
import io.warp10.script.functions.PATTERNS;
import io.warp10.script.functions.PICK;
import io.warp10.script.functions.PICKLETO;
import io.warp10.script.functions.PIGSCHEMA;
import io.warp10.script.functions.PRNG;
import io.warp10.script.functions.PROB;
import io.warp10.script.functions.PROBABILITY;
import io.warp10.script.functions.PUT;
import io.warp10.script.functions.Pi;
import io.warp10.script.functions.QCONJUGATE;
import io.warp10.script.functions.QDIVIDE;
import io.warp10.script.functions.QMULTIPLY;
import io.warp10.script.functions.QROTATE;
import io.warp10.script.functions.QROTATION;
import io.warp10.script.functions.QUANTIZE;
import io.warp10.script.functions.QUATERNIONTO;
import io.warp10.script.functions.RAND;
import io.warp10.script.functions.RANDPDF;
import io.warp10.script.functions.RANGE;
import io.warp10.script.functions.RANGECOMPACT;
import io.warp10.script.functions.REDEFS;
import io.warp10.script.functions.REDUCE;
import io.warp10.script.functions.REF;
import io.warp10.script.functions.RELABEL;
import io.warp10.script.functions.REMOVE;
import io.warp10.script.functions.REMOVETICK;
import io.warp10.script.functions.RENAME;
import io.warp10.script.functions.REOPTALT;
import io.warp10.script.functions.REPLACE;
import io.warp10.script.functions.RESET;
import io.warp10.script.functions.RESETS;
import io.warp10.script.functions.RESTORE;
import io.warp10.script.functions.RETHROW;
import io.warp10.script.functions.RETURN;
import io.warp10.script.functions.REV;
import io.warp10.script.functions.REVERSE;
import io.warp10.script.functions.REXEC;
import io.warp10.script.functions.RLOWESS;
import io.warp10.script.functions.ROLL;
import io.warp10.script.functions.ROLLD;
import io.warp10.script.functions.ROT;
import io.warp10.script.functions.ROTATIONQ;
import io.warp10.script.functions.RSADECRYPT;
import io.warp10.script.functions.RSAENCRYPT;
import io.warp10.script.functions.RSAGEN;
import io.warp10.script.functions.RSAPRIVATE;
import io.warp10.script.functions.RSAPUBLIC;
import io.warp10.script.functions.RSASIGN;
import io.warp10.script.functions.RSAVERIFY;
import io.warp10.script.functions.RSORT;
import io.warp10.script.functions.RTFM;
import io.warp10.script.functions.RUN;
import io.warp10.script.functions.RUNNERNONCE;
import io.warp10.script.functions.RVALUESORT;
import io.warp10.script.functions.SAVE;
import io.warp10.script.functions.SECTION;
import io.warp10.script.functions.SECURE;
import io.warp10.script.functions.SECUREKEY;
import io.warp10.script.functions.SET;
import io.warp10.script.functions.SETATTRIBUTES;
import io.warp10.script.functions.SETTO;
import io.warp10.script.functions.SHRINK;
import io.warp10.script.functions.SINGLEEXPONENTIALSMOOTHING;
import io.warp10.script.functions.SIZE;
import io.warp10.script.functions.SKEWNESS;
import io.warp10.script.functions.SMARTPARSE;
import io.warp10.script.functions.SNAPSHOT;
import io.warp10.script.functions.SORT;
import io.warp10.script.functions.SORTBY;
import io.warp10.script.functions.SPLIT;
import io.warp10.script.functions.SRAND;
import io.warp10.script.functions.STACKATTRIBUTE;
import io.warp10.script.functions.STACKTOLIST;
import io.warp10.script.functions.STANDARDIZE;
import io.warp10.script.functions.STL;
import io.warp10.script.functions.STLESDTEST;
import io.warp10.script.functions.STOP;
import io.warp10.script.functions.STORE;
import io.warp10.script.functions.STRICTREDUCER;
import io.warp10.script.functions.STU;
import io.warp10.script.functions.SUBLIST;
import io.warp10.script.functions.SUBMAP;
import io.warp10.script.functions.SUBSTRING;
import io.warp10.script.functions.SWAP;
import io.warp10.script.functions.SWITCH;
import io.warp10.script.functions.TEMPLATE;
import io.warp10.script.functions.THRESHOLDTEST;
import io.warp10.script.functions.TICKINDEX;
import io.warp10.script.functions.TICKLIST;
import io.warp10.script.functions.TICKS;
import io.warp10.script.functions.TIMECLIP;
import io.warp10.script.functions.TIMEMODULO;
import io.warp10.script.functions.TIMESCALE;
import io.warp10.script.functions.TIMESHIFT;
import io.warp10.script.functions.TIMESPLIT;
import io.warp10.script.functions.TIMINGS;
import io.warp10.script.functions.TOB64;
import io.warp10.script.functions.TOB64URL;
import io.warp10.script.functions.TOBYTES;
import io.warp10.script.functions.TOENCODER;
import io.warp10.script.functions.TOGEOHASH;
import io.warp10.script.functions.TOGTS;
import io.warp10.script.functions.TOHHCODE;
import io.warp10.script.functions.TOJSON;
import io.warp10.script.functions.TOKENINFO;
import io.warp10.script.functions.TOLIST;
import io.warp10.script.functions.TOLOWER;
import io.warp10.script.functions.TOMAP;
import io.warp10.script.functions.TOMAT;
import io.warp10.script.functions.TOOPB64;
import io.warp10.script.functions.TOPICKLE;
import io.warp10.script.functions.TOQUATERNION;
import io.warp10.script.functions.TOSELECTOR;
import io.warp10.script.functions.TOSET;
import io.warp10.script.functions.TOUPPER;
import io.warp10.script.functions.TOVEC;
import io.warp10.script.functions.TOVECTOR;
import io.warp10.script.functions.TOZ;
import io.warp10.script.functions.TR;
import io.warp10.script.functions.TRANSPOSE;
import io.warp10.script.functions.TRIM;
import io.warp10.script.functions.TRY;
import io.warp10.script.functions.TSELEMENTS;
import io.warp10.script.functions.TYPEOF;
import io.warp10.script.functions.UDF;
import io.warp10.script.functions.UNBUCKETIZE;
import io.warp10.script.functions.UNGZIP;
import io.warp10.script.functions.UNION;
import io.warp10.script.functions.UNIQUE;
import io.warp10.script.functions.UNLIST;
import io.warp10.script.functions.UNMAP;
import io.warp10.script.functions.UNPACK;
import io.warp10.script.functions.UNSECURE;
import io.warp10.script.functions.UNTIL;
import io.warp10.script.functions.UNWRAP;
import io.warp10.script.functions.UNWRAPENCODER;
import io.warp10.script.functions.UNWRAPSIZE;
import io.warp10.script.functions.UPDATE;
import io.warp10.script.functions.URLDECODE;
import io.warp10.script.functions.URLENCODE;
import io.warp10.script.functions.UUID;
import io.warp10.script.functions.VALUEDEDUP;
import io.warp10.script.functions.VALUEHISTOGRAM;
import io.warp10.script.functions.VALUELIST;
import io.warp10.script.functions.VALUES;
import io.warp10.script.functions.VALUESORT;
import io.warp10.script.functions.VALUESPLIT;
import io.warp10.script.functions.VECTO;
import io.warp10.script.functions.VECTORTO;
import io.warp10.script.functions.WEBCALL;
import io.warp10.script.functions.WHILE;
import io.warp10.script.functions.WRAP;
import io.warp10.script.functions.WRAPRAW;
import io.warp10.script.functions.ZSCORE;
import io.warp10.script.functions.ZSCORETEST;
import io.warp10.script.functions.ZTO;
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
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Library of functions used to manipulate Geo Time Series
 * and more generally interact with a WarpScriptStack
 */
public class WarpScriptLib {
  
  private static final Logger LOG = LoggerFactory.getLogger(WarpScriptLib.class);
  
  private static Map<String,Object> functions = new HashMap<String, Object>();
  
  private static Set<String> extloaded = new HashSet<String>();
  
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
  public static final String RUN = "RUN";
  public static final String BOOTSTRAP = "BOOTSTRAP";
  
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
  
  static {

    functions.put("REV", new REV("REV"));
    functions.put("MINREV", new MINREV("MINREV"));

    functions.put(BOOTSTRAP, new NOOP(BOOTSTRAP));

    functions.put("RTFM", new RTFM("RTFM"));
    functions.put("MAN", new MAN("MAN"));

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
    functions.put("ISAUTHENTICATED", new ISAUTHENTICATED("ISAUTHENTICATED"));
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
    functions.put(PUT, new PUT(PUT));
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
    functions.put(SAVE, new SAVE(SAVE));
    functions.put(RESTORE, new RESTORE(RESTORE));
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
    functions.put("MAXGEOCELLS", new MAXGEOCELLS("MAXGEOCELLS"));
    functions.put("MAXPIXELS", new MAXPIXELS("MAXPIXELS"));
    functions.put("MAXRECURSION", new MAXRECURSION("MAXRECURSION"));
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
    functions.put("EXTLOADED", new EXTLOADED("EXTLOADED"));
    functions.put("ASSERT", new ASSERT("ASSERT"));
    functions.put("ASSERTMSG", new ASSERTMSG("ASSERTMSG"));
    functions.put("FAIL", new FAIL("FAIL"));
    functions.put(MSGFAIL, new MSGFAIL(MSGFAIL));
    functions.put("STOP", new STOP("STOP"));
    functions.put("TRY", new TRY("TRY"));
    functions.put("RETHROW", new RETHROW("RETHROW"));
    functions.put("ERROR", new ERROR("ERROR"));
    functions.put("JSONSTRICT", new JSONSTRICT("JSONSTRICT"));
    functions.put("JSONLOOSE", new JSONLOOSE("JSONLOOSE"));
    functions.put("DEBUGON", new DEBUGON("DEBUGON"));
    functions.put("NDEBUGON", new NDEBUGON("NDEBUGON"));
    functions.put("DEBUGOFF", new DEBUGOFF("DEBUGOFF"));
    functions.put("LINEON", new LINEON("LINEON"));
    functions.put("LINEOFF", new LINEOFF("LINEOFF"));
    functions.put("LMAP", new LMAP("LMAP"));
    functions.put("NONNULL", new NONNULL("NONNULL"));
    functions.put("LFLATMAP", new LFLATMAP("LFLATMAP"));
    functions.put("[]", new EMPTYLIST("[]"));
    functions.put(LIST_START, new MARK(LIST_START));
    functions.put(LIST_END, new ENDLIST(LIST_END));
    functions.put("STACKTOLIST", new STACKTOLIST("STACKTOLIST"));
    functions.put(SET_START, new MARK(SET_START));
    functions.put(SET_END, new ENDSET(SET_END));
    functions.put("()", new EMPTYSET("()"));
    functions.put(VECTOR_START, new MARK(VECTOR_START));
    functions.put(VECTOR_END, new ENDVECTOR(VECTOR_END));
    functions.put("[[]]", new EMPTYVECTOR("[[]]"));
    functions.put("{}", new EMPTYMAP("{}"));
    functions.put("IMMUTABLE", new IMMUTABLE("IMMUTABLE"));
    functions.put(MAP_START, new MARK(MAP_START));
    functions.put(MAP_END, new ENDMAP(MAP_END));
    functions.put("SECUREKEY", new SECUREKEY("SECUREKEY"));
    functions.put("SECURE", new SECURE("SECURE"));
    functions.put("UNSECURE", new UNSECURE("UNSECURE", true));
    functions.put(EVALSECURE, new EVALSECURE(EVALSECURE));
    functions.put("NOOP", new NOOP("NOOP"));
    functions.put("DOC", new DOC("DOC"));
    functions.put("DOCMODE", new DOCMODE("DOCMODE"));
    functions.put("INFO", new INFO("INFO"));
    functions.put("INFOMODE", new INFOMODE("INFOMODE"));
    functions.put(SECTION, new SECTION(SECTION));
    functions.put("GETSECTION", new GETSECTION("GETSECTION"));
    functions.put(SNAPSHOT, new SNAPSHOT(SNAPSHOT, false, false, true, false));
    functions.put(SNAPSHOTALL, new SNAPSHOT(SNAPSHOTALL, true, false, true, false));
    functions.put("SNAPSHOTTOMARK", new SNAPSHOT("SNAPSHOTTOMARK", false, true, true, false));
    functions.put("SNAPSHOTALLTOMARK", new SNAPSHOT("SNAPSHOTALLTOMARK", true, true, true, false));
    functions.put("SNAPSHOTCOPY", new SNAPSHOT("SNAPSHOTCOPY", false, false, false, false));
    functions.put("SNAPSHOTCOPYALL", new SNAPSHOT("SNAPSHOTCOPYALL", true, false, false, false));
    functions.put("SNAPSHOTCOPYTOMARK", new SNAPSHOT("SNAPSHOTCOPYTOMARK", false, true, false, false));
    functions.put("SNAPSHOTCOPYALLTOMARK", new SNAPSHOT("SNAPSHOTCOPYALLTOMARK", true, true, false, false));
    functions.put("SNAPSHOTN", new SNAPSHOT("SNAPSHOTN", false, false, true, true));
    functions.put("SNAPSHOTCOPYN", new SNAPSHOT("SNAPSHOTCOPYN", false, false, false, true));
    functions.put("HEADER", new HEADER("HEADER"));
    
    //
    // Compilation related dummy functions
    //
    functions.put(COMPILE, new FAIL(COMPILE, "Not supported"));
    functions.put(SAFECOMPILE, new NOOP(SAFECOMPILE));
    functions.put(COMPILED, new FAIL(COMPILED, "Not supported"));
    functions.put(REF, new REF(REF));

    functions.put("MACROTTL", new MACROTTL("MACROTTL"));
    functions.put("MACROMAPPER", new MACROMAPPER("MACROMAPPER"));
    functions.put("MACROREDUCER", new MACROMAPPER("MACROREDUCER"));
    functions.put("MACROBUCKETIZER", new MACROMAPPER("MACROBUCKETIZER"));
    functions.put("MACROFILTER", new MACROFILTER("MACROFILTER"));
    functions.put("STRICTMAPPER", new STRICTMAPPER("STRICTMAPPER"));
    functions.put("STRICTREDUCER", new STRICTREDUCER("STRICTREDUCER"));
    
    functions.put(PARSESELECTOR, new PARSESELECTOR(PARSESELECTOR));
    functions.put("TOSELECTOR", new TOSELECTOR("TOSELECTOR"));
    functions.put("PARSE", new PARSE("PARSE"));
    functions.put("SMARTPARSE", new SMARTPARSE("SMARTPARSE"));
        
    // We do not expose DUMP, it might allocate too much memory
    //functions.put("DUMP", new DUMP("DUMP"));
    
    // Binary ops
    functions.put("+", new ADD("+"));
    functions.put(INPLACEADD, new INPLACEADD(INPLACEADD));
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
    
    functions.put("NEWENCODER", new NEWENCODER("NEWENCODER"));
    functions.put("CHUNKENCODER", new CHUNKENCODER("CHUNKENCODER", true));
    functions.put("->ENCODER", new TOENCODER("->ENCODER"));
    functions.put("ENCODER->", new ENCODERTO("ENCODER->"));
    functions.put("->GTS", new TOGTS("->GTS"));
    functions.put("OPTIMIZE", new OPTIMIZE("OPTIMIZE"));
    functions.put(NEWGTS, new NEWGTS(NEWGTS));
    functions.put("MAKEGTS", new MAKEGTS("MAKEGTS"));
    functions.put("ADDVALUE", new ADDVALUE("ADDVALUE", false));
    functions.put("SETVALUE", new ADDVALUE("SETVALUE", true));
    functions.put("REMOVETICK", new REMOVETICK("REMOVETICK"));
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
    functions.put("FFTWINDOW", new FFTWINDOW("FFTWINDOW"));
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
    functions.put("DTW", new DTW("DTW", true, false));
    functions.put("OPTDTW", new OPTDTW("OPTDTW"));
    functions.put("ZDTW", new DTW("ZDTW", true, true));
    functions.put("RAWDTW", new DTW("RAWDTW", false, false));
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
    functions.put("GROUPBY", new GROUPBY("GROUPBY"));
    functions.put("FILTERBY", new FILTERBY("FILTERBY"));
    functions.put("UPDATE", new UPDATE("UPDATE"));
    functions.put("META", new META("META"));
    functions.put("DELETE", new DELETE("DELETE"));
    functions.put("WEBCALL", new WEBCALL("WEBCALL"));
    functions.put("MATCH", new MATCH("MATCH"));
    functions.put("MATCHER", new MATCHER("MATCHER"));
    functions.put("REPLACE", new REPLACE("REPLACE", false));
    functions.put("REPLACEALL", new REPLACE("REPLACEALL", true));
    functions.put("REOPTALT", new REOPTALT("REOPTALT"));
    
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
    functions.put(UNWRAPENCODER, new UNWRAPENCODER(UNWRAPENCODER));
    
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
    functions.put("PAPPLY", new APPLY("PAPPLY", false));
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
    try {
      functions.put("mapper.sqrt", new MapperPow("mapper.sqrt", 0.5D));
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
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
    functions.put(GEO_WKT, new GeoWKT(GEO_WKT, false));
    functions.put(GEO_WKT_UNIFORM, new GeoWKT(GEO_WKT, true));
    functions.put(GEO_JSON, new GeoJSON(GEO_JSON, false));
    functions.put(GEO_JSON_UNIFORM, new GeoJSON(GEO_JSON, true));
    functions.put("GEO.OPTIMIZE", new GEOOPTIMIZE("GEO.OPTIMIZE"));
    functions.put(GEO_INTERSECTION, new GeoIntersection(GEO_INTERSECTION));
    functions.put(GEO_UNION, new GeoUnion(GEO_UNION));
    functions.put(GEO_DIFFERENCE, new GeoSubtraction(GEO_DIFFERENCE));
    functions.put("GEO.WITHIN", new GEOWITHIN("GEO.WITHIN"));
    functions.put("GEO.INTERSECTS", new GEOINTERSECTS("GEO.INTERSECTS"));
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
    
    functions.put(COUNTER, new COUNTER(COUNTER));
    functions.put("COUNTERVALUE", new COUNTERVALUE("COUNTERVALUE"));
    functions.put("COUNTERDELTA", new COUNTERDELTA("COUNTERDELTA"));
    functions.put(COUNTERSET, new COUNTERSET(COUNTERSET));

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
    functions.put("PRNG", new PRNG("PRNG"));
    functions.put("SRAND", new SRAND("SRAND"));

    functions.put("NPDF", new NPDF.Builder("NPDF"));
    functions.put("MUSIGMA", new MUSIGMA("MUSIGMA"));
    functions.put("KURTOSIS", new KURTOSIS("KURTOSIS"));
    functions.put("SKEWNESS", new SKEWNESS("SKEWNESS"));
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

    functions.put("COS", new COS("COS"));
    functions.put("COSH", new COSH("COSH"));
    functions.put("ACOS", new ACOS("ACOS"));

    functions.put("SIN", new SIN("SIN"));
    functions.put("SINH", new SINH("SINH"));
    functions.put("ASIN", new ASIN("ASIN"));

    functions.put("TAN", new TAN("TAN"));
    functions.put("TANH", new TANH("TANH"));
    functions.put("ATAN", new ATAN("ATAN"));

    functions.put("SIGNUM", new SIGNUM("SIGNUM"));
    functions.put("FLOOR", new FLOOR("FLOOR"));
    functions.put("CEIL", new CEIL("CEIL"));
    functions.put("ROUND", new ROUND("ROUND"));

    functions.put("RINT", new RINT("RINT"));
    functions.put("NEXTUP", new NEXTUP("NEXTUP"));
    functions.put("ULP", new ULP("ULP"));

    functions.put("SQRT", new SQRT("SQRT"));
    functions.put("CBRT", new CBRT("CBRT"));
    functions.put("EXP", new EXP("EXP"));
    functions.put("EXPM1", new EXPM1("EXPM1"));
    functions.put("LOG", new LOG("LOG"));
    functions.put("LOG10", new LOG10("LOG10"));
    functions.put("LOG1P", new LOG1P("LOG1P"));

    functions.put("TORADIANS", new TORADIANS("TORADIANS"));
    functions.put("TODEGREES", new TODEGREES("TODEGREES"));

    functions.put("MAX", new MAX("MAX"));
    functions.put("MIN", new MIN("MIN"));

    functions.put("COPYSIGN", new COPYSIGN("COPYSIGN"));
    functions.put("HYPOT", new HYPOT("HYPOT"));
    functions.put("IEEEREMAINDER", new IEEEREMAINDER("IEEEREMAINDER"));
    functions.put("NEXTAFTER", new NEXTAFTER("NEXTAFTER"));
    functions.put("ATAN2", new ATAN2("ATAN2"));

    functions.put("FLOORDIV", new FLOORDIV("FLOORDIV"));
    functions.put("FLOORMOD", new FLOORMOD("FLOORMOD"));

    functions.put("ADDEXACT", new ADDEXACT("ADDEXACT"));
    functions.put("SUBTRACTEXACT", new SUBTRACTEXACT("SUBTRACTEXACT"));
    functions.put("MULTIPLYEXACT", new MULTIPLYEXACT("MULTIPLYEXACT"));
    functions.put("INCREMENTEXACT", new INCREMENTEXACT("INCREMENTEXACT"));
    functions.put("DECREMENTEXACT", new DECREMENTEXACT("DECREMENTEXACT"));
    functions.put("NEGATEEXACT", new NEGATEEXACT("NEGATEEXACT"));
    functions.put("TOINTEXACT", new TOINTEXACT("TOINTEXACT"));

    functions.put("SCALB", new SCALB("SCALB"));
    functions.put("RANDOM", new RANDOM("RANDOM"));
    functions.put("NEXTDOWN", new NEXTDOWN("NEXTDOWN"));
    functions.put("GETEXPONENT", new GETEXPONENT("GETEXPONENT"));
    
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
    functions.put("PloadShape", new PloadShape("PloadShape"));
    functions.put("PbeginContour", new PbeginContour("PbeginContour"));
    functions.put("PendContour", new PendContour("PendContour"));
    functions.put("Pvertex", new Pvertex("Pvertex"));
    functions.put("PcurveVertex", new PcurveVertex("PcurveVertex"));
    functions.put("PbezierVertex", new PbezierVertex("PbezierVertex"));
    functions.put("PquadraticVertex", new PquadraticVertex("PquadraticVertex"));
    
    // TODO(hbs): support PShape (need to support PbeginShape etc applied to PShape instances)
    functions.put("PshapeMode", new PshapeMode("PshapeMode"));
    functions.put("Pshape", new Pshape("Pshape"));
    
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
    functions.put("Pfilter", new Pfilter("Pfilter"));

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
    functions.put("bucketizer.rms", new RMS("bucketizer.rms", false));
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
    functions.put("mapper.rms", new RMS("mapper.rms", false));

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
    functions.put("reducer.rms", new RMS("reducer.rms", false));
    functions.put("reducer.rms.exclude-nulls", new RMS("reducer.rms.exclude-nulls", true));

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
        
        System.out.print("LOADED extension '" + extension  + "'");
        
        String namespace = props.getProperty(Configuration.CONFIG_WARPSCRIPT_NAMESPACE_PREFIX + wse.getClass().getName(), "").trim(); 
        if (null != namespace && !"".equals(namespace)) {
          if (namespace.contains("%")) {
            namespace = URLDecoder.decode(namespace, "UTF-8");
          }
          System.out.println(" under namespace '" + namespace + "'.");
        } else {
          System.out.println();
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
      throw new RuntimeException(sb.toString());
    }
  }
  
  public static void register(WarpScriptExtension extension) {
    Properties props = WarpConfig.getProperties();
    
    if (null == props) {
      return;
    }

    String namespace = props.getProperty(Configuration.CONFIG_WARPSCRIPT_NAMESPACE_PREFIX + extension.getClass().getName(), "").trim();
        
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
