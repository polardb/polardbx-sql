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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RFBuilderMergeRule extends RelOptRule {

    public static final RFBuilderMergeRule INSTANCE = new RFBuilderMergeRule();

    RFBuilderMergeRule() {
        super(operand(RuntimeFilterBuilder.class,
            operand(RuntimeFilterBuilder.class, any())), "RFBuilderMergeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RuntimeFilterBuilder filterBuilder1 = call.rel(0);
        Map<String, RexNode> mergeConditions = new TreeMap<>();
        List<RexNode> conditions = RelOptUtil.conjunctions(
            filterBuilder1.getCondition());
        RuntimeFilterBuilder filterBuilder2 = call.rel(1);
        conditions.addAll(RelOptUtil.conjunctions(
            filterBuilder2.getCondition()));

        RelBuilder relBuilder = this.relBuilderFactory.create(filterBuilder1.getCluster(), null);
        for (RexNode rexNode : conditions) {
            String keyString = ((RexCall) rexNode).getOperands().toString();
            if (mergeConditions.containsKey(keyString)) {
                //update the RF-function'id.
                List<Integer> ids = new ArrayList<>();
                SqlRuntimeFilterBuildFunction val =
                    (SqlRuntimeFilterBuildFunction) (((RexCall) mergeConditions.get(keyString)).getOperator());
                ids.addAll(val.getRuntimeFilterIds());
                SqlRuntimeFilterBuildFunction repeat =
                    (SqlRuntimeFilterBuildFunction) (((RexCall) rexNode).getOperator());
                ids.addAll(repeat.getRuntimeFilterIds());
                SqlRuntimeFilterBuildFunction updateRBF = new SqlRuntimeFilterBuildFunction(ids, val.getNdv());
                mergeConditions.put(keyString, relBuilder.call(updateRBF, ((RexCall) rexNode).operands));
            } else {
                mergeConditions.put(keyString, rexNode);
            }
        }
        RelNode input = filterBuilder2.getInput();

        RuntimeFilterUtil.updateBuildFunctionNdv(input, mergeConditions.values());

        RuntimeFilterBuilder newFilterBuilder = RuntimeFilterBuilder.create(
            input,
            RexUtil.composeConjunction(filterBuilder1.getCluster().getRexBuilder(), mergeConditions.values(), true));
        call.transformTo(newFilterBuilder);

    }
}
