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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.aggregator.Argmax;
import io.warp10.script.aggregator.Argmin;
import io.warp10.script.aggregator.Count;
import io.warp10.script.aggregator.Delta;
import io.warp10.script.aggregator.First;
import io.warp10.script.aggregator.HDist;
import io.warp10.script.aggregator.HSpeed;
import io.warp10.script.aggregator.Highest;
import io.warp10.script.aggregator.Join;
import io.warp10.script.aggregator.Last;
import io.warp10.script.aggregator.Lowest;
import io.warp10.script.aggregator.Max;
import io.warp10.script.aggregator.Mean;
import io.warp10.script.aggregator.Median;
import io.warp10.script.aggregator.Min;
import io.warp10.script.aggregator.Percentile;
import io.warp10.script.aggregator.Rate;
import io.warp10.script.aggregator.ShannonEntropy;
import io.warp10.script.aggregator.StandardDeviation;
import io.warp10.script.aggregator.Sum;
import io.warp10.script.aggregator.TrueCourse;
import io.warp10.script.aggregator.VDist;
import io.warp10.script.aggregator.VSpeed;
import io.warp10.script.aggregator.Variance;
import io.warp10.script.mapper.MapperAbs;
import io.warp10.script.mapper.MapperCeil;
import io.warp10.script.mapper.MapperFinite;
import io.warp10.script.mapper.MapperFloor;
import io.warp10.script.mapper.MapperGeoClearPosition;
import io.warp10.script.mapper.MapperProduct;
import io.warp10.script.mapper.MapperRound;
import io.warp10.script.mapper.MapperSigmoid;
import io.warp10.script.mapper.MapperTanh;
import io.warp10.script.mapper.MapperToBoolean;
import io.warp10.script.mapper.MapperToDouble;
import io.warp10.script.mapper.MapperToLong;
import io.warp10.script.mapper.MapperToString;
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

import java.util.HashMap;
import java.util.Map;

public class JavaLibrary {
  
  private static final Map<String,Object> functions = new HashMap<String, Object>();
  
  static {
    //
    // Bucketizers
    //
    
    functions.put("bucketizer.first", new First("bucketizer.first"));                       // doc/einstein/bucketizer_first        Example done   Unit test
    functions.put("bucketizer.last", new Last("bucketizer.last"));                          // doc/einstein/bucketizer_last         Example done   Unit test
    functions.put("bucketizer.min", new Min("bucketizer.min", true));                       // doc/einstein/bucketizer_min          Example done   Unit test
    functions.put("bucketizer.max", new Max("bucketizer.max", true));                       // doc/einstein/bucketizer_max          Example done   Unit test
    functions.put("bucketizer.mean", new Mean("bucketizer.mean", false));                          // doc/einstein/bucketizer_mean         Example done   Unit test
    functions.put("bucketizer.median", new Median("bucketizer.median"));                    // doc/einstein/bucketizer_mean         Example done   Unit test
    functions.put("bucketizer.sum", new Sum("bucketizer.sum", true));                       // doc/einstein/bucketizer_sum          Example done   Unit test
    functions.put("bucketizer.join", new Join.Builder("bucketizer.join", true));            // doc/einstein/bucketizer_mean         Example done   Unit test
    functions.put("bucketizer.count", new Count("bucketizer.count", false));                // doc/einstein/bucketizer_count        Example done   Unit test
    functions.put("bucketizer.percentile", new Percentile.Builder("bucketizer.percentile"));
    
    //
    // Mappers
    //
    
    functions.put("mapper.count", new Count("mapper.count", false));
    functions.put("mapper.first", new First("mapper.first"));                           // doc/einstein/mapper_first        Example done   Unit test
    functions.put("mapper.last", new Last("mapper.last"));                              // doc/einstein/mapper_last         Example done   Unit test
    functions.put("mapper.min", new Min("mapper.min", true));                           // doc/einstein/mapper_min          Example done   Unit test
    functions.put("mapper.max", new Max("mapper.max", true));                           // doc/einstein/mapper_max          Example done   Unit test
    functions.put("mapper.mean", new Mean("mapper.mean", false));                              // doc/einstein/mapper_mean         Example done   Unit test
    functions.put("mapper.median", new Median("mapper.median"));                        // doc/einstein/mapper_median       Example done   Unit test
    functions.put("mapper.highest", new Highest("mapper.highest"));                     // doc/einstein/mapper_highest      Example done   Unit test
    functions.put("mapper.lowest", new Lowest("mapper.lowest"));                        // doc/einstein/mapper_lowest       Example done   Unit test
    functions.put("mapper.sum", new Sum("mapper.sum", true));                           // doc/einstein/mapper_sum          Example done   Unit test
    functions.put("mapper.join", new Join.Builder("mapper.join", true));
    functions.put("mapper.delta", new Delta("mapper.delta"));                           // doc/einstein/mapper_delta        Example done   Unit test
    functions.put("mapper.rate", new Rate("mapper.rate"));                              // doc/einstein/mapper_rate         Example done   Unit test
    functions.put("mapper.hspeed", new HSpeed("mapper.hspeed"));                        // doc/einstein/mapper_hspeed       Example done   Unit test
    functions.put("mapper.hdist", new HDist("mapper.hdist"));                           // doc/einstein/mapper_hdist        Example done   Unit test
    functions.put("mapper.truecourse", new TrueCourse("mapper.truecourse"));
    functions.put("mapper.vspeed", new VSpeed("mapper.vspeed"));                        // doc/einstein/mapper_vspeed       Example done   Unit test
    functions.put("mapper.vdist", new VDist("mapper.vdist"));                           // doc/einstein/mapper_vdist        Example done   Unit test
    functions.put("mapper.var", new Variance.Builder("mapper.var", false));                    // doc/einstein/mapper_var          Example done   Unit test
    functions.put("mapper.sd", new StandardDeviation.Builder("mapper.sd", false));             // doc/einstein/mapper_sd           Example done   Unit test
    functions.put("mapper.abs", new MapperAbs("mapper.abs"));                           // doc/einstein/mapper_abs          Example done   Unit test
    functions.put("mapper.ceil", new MapperCeil("mapper.ceil"));                        // doc/einstein/mapper_ceil         Example done   Unit test
    functions.put("mapper.floor", new MapperFloor("mapper.floor"));                     // doc/einstein/mapper_floor        Example done   Unit test
    functions.put("mapper.finite", new MapperFinite("mapper.finite"));
    functions.put("mapper.round", new MapperRound("mapper.round"));                     // doc/einstein/mapper_round        Example done   Unit test
    functions.put("mapper.toboolean", new MapperToBoolean("mapper.toboolean"));         // doc/einstein/mapper_toboolean    Example done   Unit test
    functions.put("mapper.tolong", new MapperToLong("mapper.tolong"));                  // doc/einstein/mapper_tolong       Example done   Unit test
    functions.put("mapper.todouble", new MapperToDouble("mapper.todouble"));            // doc/einstein/mapper_todouble     Example done   Unit test
    functions.put("mapper.tostring", new MapperToString("mapper.tostring"));            // doc/einstein/mapper_tostring     Example done   Unit test
    functions.put("mapper.tanh", new MapperTanh("mapper.tanh"));
    functions.put("mapper.sigmoid", new MapperSigmoid("mapper.sigmoid"));
    functions.put("mapper.product", new MapperProduct("mapper.product"));
    functions.put("mapper.geo.clear", new MapperGeoClearPosition("mapper.geo.clear"));

    //
    // Reducers
    //
    
    functions.put("reducer.min", new Min("reducer.min", true));                         // doc/einstein/reducer_min             Example done   Unit test
    functions.put("reducer.min.forbid-nulls", new Min("reducer.min.forbid-nulls", false));
    functions.put("reducer.min.nonnull", new Min("reducer.min.nonnull", false));        // doc/einstein/reducer_min.nonnull     Example done   Unit test
    functions.put("reducer.max", new Max("reducer.max", true));                         // doc/einstein/reducer_max             Example done   Unit test
    functions.put("reducer.max.forbid-nulls", new Max("reducer.max.forbid-nulls", false));
    functions.put("reducer.max.nonnull", new Max("reducer.max.nonnull", false));        // doc/einstein/reducer_max.nonnull     Example done   Unit test
    functions.put("reducer.mean", new Mean("reducer.mean", false));                            // doc/einstein/reducer_mean            Example done   Unit test
    functions.put("reducer.mean.exclude-nulls", new Mean("reducer.mean.exclude-nulls", true));
    functions.put("reducer.median", new Median("reducer.median"));                      // doc/einstein/reducer_median          Example done   Unit test
    functions.put("reducer.sum", new Sum("reducer.sum", true));                         // doc/einstein/reducer_sum             Example done   Unit test
    functions.put("reducer.sum.forbid-nulls", new Sum("reducer.sum.forbid-nulls", false));
    functions.put("reducer.sum.nonnull", new Sum("reducer.sum.nonnull", false));        // doc/einstein/reducer_sum_nonull      Example done   Unit test
    functions.put("reducer.join", new Join.Builder("reducer.join", true));              // doc/einstein/reducer_join            Example done   Unit test
    functions.put("reducer.join.forbid-nulls", new Join.Builder("reducer.join.forbid-nulls", false));
    functions.put("reducer.join.nonnull", new Join.Builder("reducer.join.nonnull", false));   // doc/einstein/reducer_sum_nonnull    Example done   Unit test
    functions.put("reducer.var", new Variance.Builder("reducer.var", false));                  // doc/einstein/reducer_var             Example done   Unit test
    functions.put("reducer.var.forbid-nulls", new Variance.Builder("reducer.var.forbid-nulls", false));
    functions.put("reducer.sd", new StandardDeviation.Builder("reducer.sd", false));           // doc/einstein/reducer_sd              Example done   Unit test
    functions.put("reducer.sd.forbid-nulls", new StandardDeviation.Builder("reducer.sd.forbid-nulls", false));           // doc/einstein/reducer_sd              Example done   Unit test
    functions.put("reducer.argmin", new Argmin.Builder("reducer.argmin"));              // doc/einstein/reducer_argmin          Example done   Unit test
    functions.put("reducer.argmax", new Argmax.Builder("reducer.argmax"));              // doc/einstein/reducer_argmax          Example done   Unit test
    functions.put("reducer.product", new MapperProduct("reducer.product"));
    functions.put("reducer.count", new Count("reducer.count", false));                  // doc/einstein/reducer_count           Example done   Unit test
    functions.put("reducer.count.include-nulls", new Count("reducer.count.include-nulls", false));
    functions.put("reducer.count.exclude-nulls", new Count("reducer.count.exclude-nulls", true));
    functions.put("reducer.count.nonnull", new Count("reducer.count.nonnull", true));         // doc/einstein/reducer_count_nonnull  Example done   Unit test
    functions.put("reducer.shannonentropy.0", new ShannonEntropy("reducer.shannonentropy.0", false));
    functions.put("reducer.shannonentropy.1", new ShannonEntropy("reducer.shannonentropy.1", true));
    functions.put("reducer.percentile", new Percentile.Builder("reducer.percentile"));
    
    //
    // Filters
    //
    
    //
    // N-ary ops
    //
    
    functions.put("op.add", new OpAdd("op.add", true));
    functions.put("op.add", new OpAdd("op.add.ignore-nulls", false));
    functions.put("op.sub", new OpSub("op.sub"));
    functions.put("op.mul", new OpMul("op.mul", true));
    functions.put("op.mul", new OpMul("op.mul.ignore-nulls", false));
    functions.put("op.div", new OpDiv("op.div"));
    functions.put("op.mask", new OpMask("op.mask"));
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
  }
  
  public static final Object getFunction(String name) {
    return functions.get(name);
  }  
}
