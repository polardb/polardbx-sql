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

package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalWindowToSortWindowRule;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;

public class SMPLogicalWindowToSortWindowRule extends LogicalWindowToSortWindowRule {

    public static final LogicalWindowToSortWindowRule INSTANCE = new SMPLogicalWindowToSortWindowRule("INSTANCE");

    public SMPLogicalWindowToSortWindowRule(String desc) {
        super("SMP_" + desc);
    }

    protected void createSortWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput,
        RelCollation relCollation) {
        if (relCollation != RelCollations.EMPTY) {
            LogicalSort sort =
                LogicalSort.create(newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation), newInput,
                    relCollation, null, null);
            newInput = convert(sort, sort.getTraitSet().replace(outConvention));
        }

        SortWindow newWindow =
            SortWindow.create(
                window.getTraitSet().replace(outConvention).replace(relCollation),
                newInput,
                window.getConstants(),
                window.groups,
                window.getRowType());
        call.transformTo(newWindow);
    }
}
