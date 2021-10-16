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
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class LogicalWindowToSortWindowRule extends RelOptRule {

    public static final LogicalWindowToSortWindowRule INSTANCE = new LogicalWindowToSortWindowRule();

    public LogicalWindowToSortWindowRule() {
        super(operand(LogicalWindow.class, some(operand(RelSubset.class, any()))), "LogicalWindowToSortWindowRule");
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = (LogicalWindow) call.rels[0];
        RelNode input = call.rels[1];
        ImmutableBitSet groupSets = window.groups.get(0).keys;
        List<Integer> sortFields = groupSets.toList();
        List<RelFieldCollation> orderKeys = window.groups.get(0).orderKeys.getFieldCollations();

        RelCollation relCollation = RelCollations.EMPTY;
        if (groupSets.cardinality() + orderKeys.size() > 0) {
            relCollation = CBOUtil.createRelCollation(sortFields, orderKeys);
        }

        RelNode newInput = convert(input, input.getTraitSet().replace(DrdsConvention.INSTANCE).replace(relCollation));

        SortWindow newWindow =
            SortWindow.create(
                window.getTraitSet().replace(DrdsConvention.INSTANCE).replace(relCollation),
                newInput,
                window.getConstants(),
                window.groups,
                window.getRowType(),
                window.getFixedCost());
        call.transformTo(newWindow);
    }

}
