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

import com.google.common.base.Predicate;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.runtime.PredicateImpl;

/**
 * @author dylan
 */
public class OptimizeLogicalTableLookupRule extends RelOptRule {

    private static final Predicate<LogicalTableLookup> TABLE_LOOKUP_RIGHT_IS_NOT_LOGICALVIEW =
        new PredicateImpl<LogicalTableLookup>() {
            @Override
            public boolean test(LogicalTableLookup tableLookup) {
                return !(tableLookup.getJoin().getRight() instanceof LogicalView);
            }
        };

    public static final OptimizeLogicalTableLookupRule INSTANCE = new OptimizeLogicalTableLookupRule();

    protected OptimizeLogicalTableLookupRule() {
        super(operand(LogicalTableLookup.class, null, TABLE_LOOKUP_RIGHT_IS_NOT_LOGICALVIEW, RelOptRule.any()),
            "OptimizeLogicalTableLookupRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableLookup logicalTableLookup = call.rel(0);
        RelNode right = logicalTableLookup.getJoin().getRight();

        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        // push filter
        builder.addRuleInstance(PushFilterRule.LOGICALVIEW);
        builder.addRuleInstance(PushFilterRule.MERGE_SORT);
        builder.addRuleInstance(PushFilterRule.LOGICALUNION);
        // push project
        builder.addRuleInstance(PushProjectRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), PlannerContext.getPlannerContext(logicalTableLookup));
        hepPlanner.stopOptimizerTrace();
        hepPlanner.setRoot(right);
        RelNode newRight = hepPlanner.findBestExp();

        LogicalTableLookup newLogicalTableLookup = logicalTableLookup.copy(
            logicalTableLookup.getJoin().getJoinType(),
            logicalTableLookup.getJoin().getCondition(),
            logicalTableLookup.getProject().getChildExps(),
            logicalTableLookup.getRowType(),
            logicalTableLookup.getJoin().getLeft(),
            newRight,
            logicalTableLookup.isRelPushedToPrimary());

        call.transformTo(newLogicalTableLookup);
    }
}

