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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.core.planner.rule.PushFilterRule.doNotPush;

public class PushBloomFilterRule extends RelOptRule {
    public static final PushBloomFilterRule LOGICALVIEW = new PushBloomFilterRule(
        operand(Filter.class, operand(LogicalView.class, none())), "LOGICALVIEW");

    public PushBloomFilterRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushBloomFilterRule:" + description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getExecutionContext()
            .getParamManager()
            .getBoolean(ConnectionParams.STORAGE_SUPPORTS_BLOOM_FILTER);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LogicalView logicalView = call.rel(1);

        if (doNotPush(filter, logicalView)) {
            return;
        }

        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());

        List<RexNode> pushToLogicalViewConditions = new ArrayList<>(conditions.size());
        Set<Integer> pushToLogicalViewRuntimeFilterIdSet = new HashSet<>();
        List<RexNode> reservedConditions = new ArrayList<>(conditions.size());

        for (RexNode condition : conditions) {
            boolean canPushDown = true;
            if (condition instanceof RexCall && (((RexCall) condition)
                .getOperator() instanceof SqlRuntimeFilterFunction)) {
                RelNode filterInput = filter.getInput();
                canPushDown = ((RexCall) condition).getOperands().stream()
                    .map(input -> (RexSlot) input)
                    .map(RexSlot::getIndex)
                    .map(idx -> filterInput.getRowType().getFieldList().get(idx).getType())
                    .map(DataTypeUtil::calciteToDrdsType)
                    .allMatch(logicalView instanceof OSSTableScan ? RuntimeFilterUtil::canPushRuntimeFilterToOss : RuntimeFilterUtil::canPushRuntimeFilterToMysql);
                pushToLogicalViewRuntimeFilterIdSet.add(((SqlRuntimeFilterFunction) ((RexCall) condition).getOperator()).getId());
            }

            if (canPushDown) {
                pushToLogicalViewConditions.add(condition);
            } else {
                reservedConditions.add(condition);
            }
        }

        if (pushToLogicalViewConditions.isEmpty()) {
            return;
        }

        RelNode result;
        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(Convention.NONE));

        Filter newFilter =
            filter.copy(filter.getTraitSet(),
                filter.getInput(), RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
                    pushToLogicalViewConditions, false));
        if (logicalView instanceof OSSTableScan) {
            if (newLogicalView.getBloomFilters().containsAll(pushToLogicalViewRuntimeFilterIdSet)) {
                return;
            }
            ((OSSTableScan) newLogicalView).pushRuntimeFilter(newFilter.getCondition());
            // with agg, we can't push runtime filter
            if (((OSSTableScan)newLogicalView).withAgg()) {
                return;
            }
            result = filter.copy(filter.getTraitSet(), newLogicalView,
                RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), conditions, false));
            call.transformTo(result);
            return;
        } else {
            newLogicalView.push(newFilter);
        }

        if (!reservedConditions.isEmpty()) {
            result = filter.copy(filter.getTraitSet(), newLogicalView,
                RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), reservedConditions, false));
        } else {
            result = newLogicalView;
        }

        call.transformTo(result);
    }
}
