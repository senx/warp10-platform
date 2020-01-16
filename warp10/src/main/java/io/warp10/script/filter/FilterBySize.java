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

package io.warp10.script.filter;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterBySize extends NamedWarpScriptFunction implements WarpScriptFilterFunction {
    static private final String MIN = "min";
    static private final String MAX = "max";

    private final Long min;
    private final Long max;

    public static class Builder extends FormattedWarpScriptFunction {

        private final Arguments args;

        public Builder(String name) {
            super(name);

            args = new ArgumentsBuilder()
                    .addArgument(Long.class, MIN, "The minimum size.")
                    .addArgument(Long.class, MAX, "The maximum size.")
                    .build();
        }

        @Override
        protected Arguments getArguments() {
            return args;
        }

        @Override
        protected WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException {
            Long min = (Long) formattedArgs.get(MIN);
            Long max = (Long) formattedArgs.get(MAX);
            stack.push(new FilterBySize(getName(), min, max));
            return null;
        }
    }

    public FilterBySize(String name, Long min, Long max) throws WarpScriptException {
        super(name);
        this.min = min;
        this.max = max;
    }

    @Override
    public List<GeoTimeSerie> filter(Map<String, String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {

        List<GeoTimeSerie> retained = new ArrayList<GeoTimeSerie>();

        for (List<GeoTimeSerie> serie: series) {
            for (GeoTimeSerie gts: serie) {
                if (gts.size() >= min && gts.size() <= max) {
                    retained.add(gts);
                }
            }
        }

        return retained;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StackUtils.toString(min));
        sb.append(StackUtils.toString(max));
        sb.append(" ");
        sb.append(this.getName());
        return sb.toString();
    }
}
