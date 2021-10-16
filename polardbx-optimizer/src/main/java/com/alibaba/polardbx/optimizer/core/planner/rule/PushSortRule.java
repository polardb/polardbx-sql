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
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.tools.RelBuilderFactory;

public class PushSortRule extends RelOptRule {

    public static final PushSortRule PLAN_ENUERATE = new PushSortRule(RelFactories.LOGICAL_BUILDER, true);

    public static final PushSortRule SQL_REWRITE = new PushSortRule(RelFactories.LOGICAL_BUILDER, false);

    private boolean planEnumerate;

    public PushSortRule(RelBuilderFactory relBuilderFactory, boolean planEnumerate) {
        super(operand(Sort.class, operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none())),
            relBuilderFactory, "PushSortRule:LOGICALVIEW");
        this.planEnumerate = planEnumerate;
    }

    @Override
    public Convention getOutConvention() {
        if (planEnumerate) {
            return DrdsConvention.INSTANCE;
        } else {
            return null;
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        if (!PlannerContext.getPlannerContext(sort).getParamManager().getBoolean(ConnectionParams.ENABLE_PUSH_SORT)) {
            return false;
        }
        return sort instanceof MemSort || sort instanceof TopN || sort instanceof Limit;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final LogicalView logicalView = call.rel(1);

        if (logicalView.isSingleGroup()) {
            RelNode sortedLogicalView = CBOUtil.sortLimitSingleGroupLogicalView(logicalView, sort);
            if (planEnumerate) {
                sortedLogicalView =
                    convert(sortedLogicalView, sortedLogicalView.getTraitSet().replace(DrdsConvention.INSTANCE));
                call.transformTo(sortedLogicalView);
            } else {
                call.transformTo(sortedLogicalView);
            }

        } else {
            // sortLimitLogicalView will convert logicalView to DrdsConvention
            LogicalView sortedLogicalView = CBOUtil.sortLimitLogicalView(logicalView, sort);
            RelNode mergeSort = MergeSort.create(sort.getTraitSet(), sortedLogicalView, sort.getCollation(),
                sort.offset,
                sort.fetch);
            if (planEnumerate) {
                mergeSort = convert(mergeSort, mergeSort.getTraitSet().replace(DrdsConvention.INSTANCE));
                call.transformTo(mergeSort);
            } else {
                call.transformTo(mergeSort);
            }
        }
    }
}

