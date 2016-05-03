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
    
    functions.put("bucketizer.first", new First("bucketizer.first"));
    functions.put("bucketizer.last", new Last("bucketizer.last"));
    functions.put("bucketizer.min", new Min("bucketizer.min", true));
    functions.put("bucketizer.max", new Max("bucketizer.max", true));
    functions.put("bucketizer.mean", new Mean("bucketizer.mean", false));
    functions.put("bucketizer.median", new Median("bucketizer.median"));
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
    
    functions.put("mapper.count", new Count("mapper.count", false));
    functions.put("mapper.first", new First("mapper.first"));
    functions.put("mapper.last", new Last("mapper.last"));
    functions.put("mapper.min", new Min("mapper.min", true));
    functions.put("mapper.max", new Max("mapper.max", true));
    functions.put("mapper.mean", new Mean("mapper.mean", false));
    functions.put("mapper.median", new Median("mapper.median"));
    functions.put("mapper.highest", new Highest("mapper.highest"));
    functions.put("mapper.lowest", new Lowest("mapper.lowest"));
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
    
    //
    // Reducers
    //
    
    functions.put("reducer.min", new Min("reducer.min", true));
    functions.put("reducer.min.forbid-nulls", new Min("reducer.min.forbid-nulls", false));
    functions.put("reducer.min.nonnull", new Min("reducer.min.nonnull", false));
    functions.put("reducer.max", new Max("reducer.max", true));
    functions.put("reducer.max.forbid-nulls", new Max("reducer.max.forbid-nulls", false));
    functions.put("reducer.max.nonnull", new Max("reducer.max.nonnull", false));
    functions.put("reducer.mean", new Mean("reducer.mean", false));
    functions.put("reducer.mean.exclude-nulls", new Mean("reducer.mean.exclude-nulls", true));
    functions.put("reducer.median", new Median("reducer.median"));
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
