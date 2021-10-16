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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;

import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class LogicalValuesToLogicalDynamicValuesRule extends RelOptRule {
    public LogicalValuesToLogicalDynamicValuesRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final LogicalValuesToLogicalDynamicValuesRule INSTANCE =
        new LogicalValuesToLogicalDynamicValuesRule(operand(LogicalInsert.class,
            operand(LogicalValues.class, none())), "LogicalValuesToLogicalDynamicValuesRule");

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalInsert insert = call.rel(0);
        final LogicalValues values = call.rel(1);

        final ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.copyOf(values.getTuples().stream().map(
            ImmutableList::<RexNode>copyOf).collect(Collectors.toList()));

        final LogicalDynamicValues dynamicValues =
            LogicalDynamicValues
                .createDrdsValues(values.getCluster(), values.getTraitSet(), insert.getInsertRowType(), tuples);

        final RelNode newInsert = insert.copy(insert.getTraitSet(), ImmutableList.of(dynamicValues));

        call.transformTo(newInsert);
    }
}
