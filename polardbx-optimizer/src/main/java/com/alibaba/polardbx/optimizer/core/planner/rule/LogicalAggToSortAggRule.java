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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

import java.util.LinkedList;
import java.util.List;

public class LogicalAggToSortAggRule extends ConverterRule {

    public static final LogicalAggToSortAggRule INSTANCE = new LogicalAggToSortAggRule();

    public LogicalAggToSortAggRule() {
        super(LogicalAggregate.class, Convention.NONE, DrdsConvention.INSTANCE, "LogicalAggToSortAggRule");
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SORT_AGG);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (CBOUtil.containUnpushableAgg(call.rel(0))) {
            return false;
        }
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate agg = (LogicalAggregate) rel;
        RelNode input = agg.getInput();

        if (agg.getAggOptimizationContext().isFromDistinctAgg() || PlannerUtils.haveAggWithDistinct(
            agg.getAggCallList())) {
            return null;
        }
        List<Integer> groupSet = Lists.newArrayList(agg.getGroupSet().asList());

        RelTraitSet inputTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);

        final RelNode newInput;
        List<RelFieldCollation> sortAggCollation = new LinkedList<>();
        if (groupSet.size() != 0) {
            List<RelFieldCollation> sortInputCollation = new LinkedList<>();
            for (int i = 0; i < groupSet.size(); i++) {
                sortInputCollation.add(new RelFieldCollation(groupSet.get(i),
                    RelFieldCollation.Direction.ASCENDING,
                    RelFieldCollation.NullDirection.FIRST));
                sortAggCollation.add(new RelFieldCollation(i,
                    RelFieldCollation.Direction.ASCENDING,
                    RelFieldCollation.NullDirection.FIRST));
            }
            newInput = convert(input, inputTraitSet.replace(RelCollations.of(sortInputCollation)));
        } else {
            newInput = convert(input, inputTraitSet);
        }

        SortAgg sortAgg = SortAgg.create(
            agg.getTraitSet().replace(RelCollations.of(sortAggCollation)).replace(DrdsConvention.INSTANCE),
            newInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());

        return sortAgg;
    }
}
