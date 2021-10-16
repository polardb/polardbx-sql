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

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;

/**
 * push sort through table lookup
 *
 * @author chenmo.cm
 */
public class SortTableLookupTransposeRule extends RelOptRule {

    public static final SortTableLookupTransposeRule INSTANCE =
        new SortTableLookupTransposeRule(
            operand(Sort.class,
                operand(LogicalTableLookup.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            "SortTableLookupTransposeRule");

    public SortTableLookupTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, RelFactories.LOGICAL_BUILDER, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final LogicalTableLookup tableLookup = call.rel(1);
        return sort.getConvention() == DrdsConvention.INSTANCE
            && tableLookup.getConvention() == DrdsConvention.INSTANCE
            && (sort instanceof MemSort || sort instanceof Limit || sort instanceof TopN);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final LogicalTableLookup tableLookup = call.rel(1);

        /*
         * Sort project transpose
         */
        final Project project = tableLookup.getProject();

        SortProjectTranspose sortProjectTranspose = new SortProjectTranspose(sort, project).invoke();
        if (sortProjectTranspose.isSkipped()) {
            return;
        }
        final Sort newSort = sortProjectTranspose.getNewSort();
        final Project newProject = sortProjectTranspose.getNewProject();


        /*
         * Sort join transpose
         */
        final Join join = tableLookup.getJoin();

        final boolean splitLimitAndSort;
        // If condition has been pushed to primary, means tableLookup may returns less row than indexScan.
        // If limit also exists, should not proceed.
        if (newSort.withLimit() && !newSort.withOrderBy() && tableLookup.isRelPushedToPrimary()) {
            return;
        } else if (newSort.withLimit() && newSort.withOrderBy() && tableLookup.isRelPushedToPrimary()) {
            // let the sort down and preserve limit
            splitLimitAndSort = true;
        } else {
            // let sort and limit down
            splitLimitAndSort = false;
        }

        // If order by right input column, skip.
        for (RelFieldCollation relFieldCollation : newSort.getCollation().getFieldCollations()) {
            if (relFieldCollation.getFieldIndex() >= join.getLeft().getRowType().getFieldList().size()) {
                return;
            }
        }

        // Push sort/limit
        final RelNode newLeftInput;
        if (splitLimitAndSort) {
            LogicalSort logicalSort = LogicalSort.create(join.getLeft(), newSort.getCollation(), null, null);
            newLeftInput = convert(logicalSort, logicalSort.getTraitSet().replace(DrdsConvention.INSTANCE));
        } else {
            LogicalSort logicalSort = LogicalSort.create(join.getLeft(), newSort.getCollation(), newSort.offset,
                newSort.fetch);
            newLeftInput = convert(logicalSort, logicalSort.getTraitSet().replace(DrdsConvention.INSTANCE));
        }

        RelNode result = tableLookup.copy(sort.getTraitSet().replace(sort.getCollation()).replace(
            RelDistributions.SINGLETON),
            newLeftInput, tableLookup.getJoin().getRight(),
            tableLookup.getIndexTable(),
            tableLookup.getPrimaryTable(),
            newProject,
            join.copy(join.getTraitSet().replace(newSort.getCollation()).replace(RelDistributions.SINGLETON),
                join.getInputs()),
            tableLookup.isRelPushedToPrimary(),
            tableLookup.getHints());

        if (splitLimitAndSort) {
            result = Limit.create(sort.getTraitSet(), result, newSort.offset, newSort.fetch);
        }

        call.transformTo(result);
    }
}
