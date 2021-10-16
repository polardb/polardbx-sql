/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.sharding.result;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.mapping.Mapping;

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import com.alibaba.polardbx.optimizer.sharding.label.FullRowType;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.LabelType;

/**
 * @author chenmo.cm
 * @date 2020/2/25 11:59 上午
 */
public class ColumnEqualityConditionResult extends EmptyConditionResult {

    protected final ExtractorContext context;
    protected final Label            label;

    public static ColumnEqualityConditionResult create(ExtractorContext context, Label label) {
        return new ColumnEqualityConditionResult(context, label);
    }

    protected ColumnEqualityConditionResult(ExtractorContext context, Label label){
        this.context = context;
        this.label = label;
    }

    @Override
    public Map<Integer, BitSet> toColumnEquality() {
        final FullRowType fullRowType = label.getFullRowType();
        final Mapping fullColumnMapping = fullRowType.getFullColumnMapping();

        final Map<Integer, BitSet> result = new HashMap<>();
        IntStream.range(0, fullColumnMapping.getSourceCount()).forEach(i -> result.put(i, BitSets.of(i)));

        if (label.getType() != LabelType.JOIN) {
            return result;
        }

        if (fullRowType.getColumnEqualities().size() > 0) {
            //final List<BitSet> groups = new ArrayList<>();
            //
            //// Add column equalities
            //for (RexNode equality : fullRowType.getColumnEqualities().values()) {
            //    if (equality instanceof RexCall) {
            //        RexCall call = (RexCall) equality;
            //        int left = PredicateUtil.pos(call.getOperands().get(0));
            //        int right = PredicateUtil.pos(call.getOperands().get(1));
            //
            //        if (left != -1 && right != -1) {
            //            groups.add(BitSets.of(left, right));
            //        }
            //    }
            //}

            // //Merge equality group
            //List<BitSet> merged = Util.mergeIntersectedSets(groups);

            // Update result
            final Mapping inverse = fullColumnMapping.inverse();

            //merged.forEach(g -> {
            //    final List<Integer> ce = g.stream()
            //        .mapToObj(inverse::getTargetOpt)
            //        .filter(i -> i >= 0)
            //        .collect(Collectors.toList());
            //
            //    final BitSet bitSet = BitSets.of(ce);
            //
            //    ce.forEach(i -> result.get(i).or(bitSet));
            //});

            for (RexNode equality : fullRowType.getColumnEqualities().values()) {
                if (equality instanceof RexCall) {
                    RexCall call = (RexCall) equality;
                    int left = PredicateUtil.pos(call.getOperands().get(0));
                    int right = PredicateUtil.pos(call.getOperands().get(1));

                    if (left != -1 && right != -1) {
                        left = inverse.getTargetOpt(left);
                        right = inverse.getTargetOpt(right);

                        if (left != -1 && right != -1) {
                            result.get(left).set(right);
                            result.get(right).set(left);
                        }
                    }
                }
            }
        }

        return result;
    }

}
