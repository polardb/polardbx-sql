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

import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

public class MergeSortRemoveGatherRule extends RelOptRule {

    public MergeSortRemoveGatherRule(RelOptRuleOperand operand) {
        super(operand, "MergeSortRemoveGatherRule");
    }

    public static final MergeSortRemoveGatherRule INSTANCE = new MergeSortRemoveGatherRule(
        operand(MergeSort.class, operand(Gather.class, operand(LogicalView.class, none()))));

    @Override
    public void onMatch(RelOptRuleCall call) {
        MergeSort mergeSort = (MergeSort) call.rels[0];
        LogicalView input = (LogicalView) call.rels[2];

        MergeSort newMergeSort =
            mergeSort.copy(mergeSort.getTraitSet(), input, mergeSort.getCollation(), mergeSort.offset, mergeSort.fetch);

        call.transformTo(newMergeSort);
    }
}

