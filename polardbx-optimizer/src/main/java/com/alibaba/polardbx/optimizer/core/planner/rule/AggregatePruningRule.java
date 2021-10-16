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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AggregatePruning
 *
 * @author hongxi.chx
 */
public class AggregatePruningRule extends RelOptRule {

    public AggregatePruningRule(RelOptRuleOperand operand, String description) {
        super(operand, "aggregate_pruning_rule:" + description);
    }

    public static final AggregatePruningRule INSTANCE = new AggregatePruningRule(
        operand(LogicalAggregate.class, any()), "AggregatePruningRule");

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate logicalAggregate = (LogicalAggregate) call.rels[0];
        if (CBOUtil.isGroupSets(logicalAggregate)) {
            return false;
        }
        return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate logicalAggregate = (LogicalAggregate) call.rels[0];
        final ParamManager paramManager = PlannerContext.getPlannerContext(logicalAggregate).getParamManager();
        boolean enable = paramManager.getBoolean(ConnectionParams.ENABLE_AGG_PRUNING);
        if (!enable) {
            return;
        }
        final RelMetadataQuery mq = logicalAggregate.getCluster().getMetadataQuery();
        Map<ImmutableBitSet, ImmutableBitSet> functionalDependency =
            mq.getFunctionalDependency(logicalAggregate.getInput(), logicalAggregate.getGroupSet());
        ImmutableBitSet bestKeySet = null;
        if (functionalDependency != null && functionalDependency.size() > 0) {
            for (ImmutableBitSet keySet : functionalDependency.keySet()) {
                if (bestKeySet == null) {
                    bestKeySet = keySet;
                } else {
                    if (keySet.cardinality() > bestKeySet.cardinality() && keySet.cardinality() < logicalAggregate
                        .getGroupSet().cardinality()) {
                        bestKeySet = keySet;
                    }
                }
            }
        }
        if (bestKeySet == null || bestKeySet.cardinality() == 0) {
            return;
        }

        List<Integer> pruningList = functionalDependency.get(bestKeySet).toList();
        List<Integer> newGroupIndexs = logicalAggregate.getGroupSet().toList();
        newGroupIndexs.removeAll(pruningList);
        if (pruningList.isEmpty() || newGroupIndexs.isEmpty()) {
            return;
        }
        newGroupIndexs.sort((i, j) -> i - j);
        pruningList.sort((i, j) -> i - j);
        ImmutableBitSet bitSet = ImmutableBitSet.of(newGroupIndexs);
        if (bitSet.equals(logicalAggregate.getGroupSet())) {
            return;
        }
        List<AggregateCall> aggregateCalls = new ArrayList<>();
        for (Integer t : pruningList) {
            final List<RelDataTypeField> fieldList = logicalAggregate.getInput().getRowType().getFieldList();
            final AggregateCall aggregateCall =
                AggregateCall.create(SqlStdOperatorTable.__FIRST_VALUE,
                    false,
                    false,
                    ImmutableIntList.of(t),
                    -1,
                    fieldList.get(t).getType(),
                    fieldList.get(t).getName());
            aggregateCalls.add(aggregateCall);
        }
        aggregateCalls.addAll(logicalAggregate.getAggCallList());
        final LogicalAggregate newAgg = LogicalAggregate.create(logicalAggregate.getInput(),
            bitSet,
            ImmutableList.of(bitSet),
            aggregateCalls);

        List<RexNode> projects = new ArrayList<>();
        List<Integer> refIndexs = new ArrayList<>();
        refIndexs.addAll(newGroupIndexs);
        refIndexs.addAll(pruningList);
        int index = 0;
        for (int i : logicalAggregate.getGroupSet()) {
            projects.add(RexInputRef.of(refIndexs.indexOf(i), newAgg.getRowType().getFieldList()));
            ++index;
        }
        for (int i = 0; i < logicalAggregate.getAggCallList().size(); i++) {
            projects.add(RexInputRef.of(index++, newAgg.getRowType().getFieldList()));
        }
        final LogicalProject rel = LogicalProject.create(newAgg, projects, logicalAggregate.getRowType());
        call.transformTo(rel);
    }

}


