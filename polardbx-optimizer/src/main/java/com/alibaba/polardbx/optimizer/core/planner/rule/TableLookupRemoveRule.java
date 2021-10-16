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

import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

/**
 * @author chenmo.cm
 */
public class TableLookupRemoveRule extends RelOptRule {

    public static final TableLookupRemoveRule INSTANCE = new TableLookupRemoveRule();

    public TableLookupRemoveRule() {
        super(operand(LogicalTableLookup.class, any()), "TableLookupRemoveRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalTableLookup tableLookup = call.rel(0);

        if (!removable(tableLookup)) {
            return;
        }

        final RelBuilder builder = call.builder();
        builder.push(tableLookup.getJoin().getLeft());
        builder.project(tableLookup.getProject().getProjects(), tableLookup.getRowType().getFieldNames());

        call.transformTo(builder.build());
    }

    private boolean removable(LogicalTableLookup tableLookup) {
        if (tableLookup.isRelPushedToPrimary()) {
            return false;
        }

        LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
        tableLookup.getJoin().getRight().accept(logicalViewFinder);
        if (logicalViewFinder.getResult().size() == 1) {
            SqlSelect.LockMode lockMode = logicalViewFinder.getResult().get(0).getLockMode();
            if (lockMode != null && lockMode != SqlSelect.LockMode.UNDEF) {
                return false;
            }
        }

        final LogicalProject project = tableLookup.getProject();
        final LogicalJoin join = tableLookup.getJoin();

        if (Util.projectHasSubQuery(project)) {
            return false;
        }

        ImmutableBitSet leftBitmap = ImmutableBitSet.range(0, join.getLeft().getRowType().getFieldCount());

        return project.getProjects().stream().allMatch(p -> {
            final InputFinder inputFinder = InputFinder.analyze(p);
            final ImmutableBitSet inputBits = inputFinder.inputBitSet.build();

            return leftBitmap.contains(inputBits);
        });
    }
}
