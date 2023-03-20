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

import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

/**
 * Created by lingce.ldm on 2016/11/2.
 */
public class PushFilterRule extends RelOptRule {

    public static final int APPLY_CACHE_ROW_COUNT = 100000;

    public PushFilterRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushFilterRule:" + description);
    }

    public static final PushFilterRule LOGICALVIEW = new PushFilterRule(
        operand(Filter.class, operand(LogicalView.class, none())), "LOGICALVIEW");

    public static final PushFilterRule MERGE_SORT = new PushFilterMergeSortTransposeRule(
        operand(Filter.class, operand(MergeSort.class, operand(LogicalView.class, none()))),
        "MERGE_SORT");

    public static final PushFilterRule LOGICALUNION = new PushFilterUnionTransposeRule(
        operand(Filter.class, operand(LogicalUnion.class, none())), "LOGICALUNION");

    public static final PushFilterRule PROJECT_FILTER_LOGICALVIEW =
        new PushProjectFilterLogicalViewRule(
            operand(Project.class, operand(Filter.class, operand(LogicalView.class, none()))),
            "PROJECT_FILTER_LOGICALVIEW");

    public static final PushFilterVirtualViewRule VIRTUALVIEW = new PushFilterVirtualViewRule(
        operand(Filter.class, operand(VirtualView.class, none())), "VIRTUALVIEW");

    // <pushable, unPushable>
    public Pair<RexNode, RexNode> splitConditionByPushable(RexNode condition, LogicalView logicalView) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
        RexNode pushable = null;
        RexNode unPushable = null;
        RexBuilder rb = logicalView.getCluster().getRexBuilder();
        for (RexNode conjunction : conjunctions) {
            if (doNotPush(conjunction, logicalView)) {
                unPushable = (unPushable == null ?
                    conjunction : RexUtil.flatten(rb, rb.makeCall(AND, unPushable, conjunction)));
            } else {
                pushable = (pushable == null ?
                    conjunction : RexUtil.flatten(rb, rb.makeCall(AND, pushable, conjunction)));
            }
        }
        return Pair.of(pushable, unPushable);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LogicalView logicalView = call.rel(1);
        PlannerContext context = call.getPlanner().getContext().unwrap(PlannerContext.class);
        if (RelUtils.isNotPushLastInsertId(context, filter)) {
            return;
        }
        if (logicalView instanceof OSSTableScan
            && !((OSSTableScan) logicalView).canPushFilterProject()) {
            return;
        }
        Pair<RexNode, RexNode> pair = splitConditionByPushable(filter.getCondition(), logicalView);
        RexNode pushable = pair.getKey();
        RexNode unPushable = pair.getValue();
        if (pushable == null && unPushable == null) {
            // always true. like true AND true
            call.transformTo(logicalView);
        } else if (pushable == null) {
            return;
        } else if (unPushable == null) {
            Filter newFilter =
                filter.copy(filter.getTraitSet(), filter.getInput(), pushable);
            // make convention none to match push (semi)join rule
            LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(Convention.NONE));
            newLogicalView.push(newFilter);
            call.transformTo(newLogicalView);
        } else {
            Filter pushableFilter =
                filter.copy(filter.getTraitSet(), filter.getInput(), pushable);
            // make convention none to match push (semi)join rule
            LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(Convention.NONE));
            newLogicalView.push(pushableFilter);
            Filter unPushableFilter =
                filter.copy(filter.getTraitSet(), newLogicalView, unPushable);
            call.transformTo(unPushableFilter);
        }
    }

    public static boolean doNotPush(Filter filter, LogicalView logicalView) {
        return doNotPush(filter.getCondition(), logicalView);
    }

    public static boolean doNotPush(RexNode condition, LogicalView logicalView) {
        if (RexUtils.containsUnPushableFunction(condition, false)) {
            return true;
        }
        if (logicalView instanceof OSSTableScan
            && !((OSSTableScan) logicalView).canPushFilterProject()) {
            return true;
        }

        if (logicalView != null) {
            /**
             * Dont push filter to lv if it's suitble for apply cache.
             */
            boolean forceCache = PlannerContext.getPlannerContext(logicalView)
                .getParamManager()
                .getBoolean(ConnectionParams.FORCE_APPLY_CACHE);

            boolean forbidCache = PlannerContext.getPlannerContext(logicalView)
                .getParamManager()
                .getBoolean(ConnectionParams.FORBID_APPLY_CACHE);
            if (!forbidCache) {
                if (forceCache
                    || logicalView.getCluster().getMetadataQuery().getRowCount(logicalView) <= APPLY_CACHE_ROW_COUNT) {
                    RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
                    condition.accept(dynamicFinder);

                    if (dynamicFinder.getCorrelateVariableScalar().size() > 0) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static class PushFilterMergeSortTransposeRule extends PushFilterRule {

        public PushFilterMergeSortTransposeRule(RelOptRuleOperand operand, String filter_mergeSort_tableScan) {
            super(operand, filter_mergeSort_tableScan);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final LogicalView logicalView = call.rel(2);
            if (logicalView instanceof OSSTableScan) {
                return false;
            }
            return true;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            MergeSort mergeSort = call.rel(1);
            LogicalView logicalView = call.rel(2);

            if (PushFilterRule.doNotPush(filter, logicalView)) {
                return;
            }

            if (null != mergeSort.fetch || null != mergeSort.offset) {
                // Cannot transpose filter with limit
                return;
            }

            Filter newLogicalFilter =
                filter.copy(filter.getTraitSet(), filter.getInput(), filter.getCondition());
            LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet());
            newLogicalView.push(newLogicalFilter);
            MergeSort newMergeSort = mergeSort
                .copy(mergeSort.getTraitSet(), newLogicalView, mergeSort.getCollation(), mergeSort.offset,
                    mergeSort.fetch);

            call.transformTo(newMergeSort);
        }
    }

    private static class PushFilterUnionTransposeRule extends PushFilterRule {

        public PushFilterUnionTransposeRule(RelOptRuleOperand operand, String filter_mergeSort_tableScan) {
            super(operand, filter_mergeSort_tableScan);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = (Filter) call.rels[0];
            LogicalUnion logicalUnion = (LogicalUnion) call.rels[1];

            if (doNotPush(filter, null)) {
                return;
            }

            List<RelNode> newInputs = Lists.newArrayList();
            for (RelNode input : logicalUnion.getInputs()) {
                Filter newFilter = filter.copy(filter.getTraitSet(), input, filter.getCondition());
                newInputs.add(newFilter);
            }

            LogicalUnion newUnion = LogicalUnion.create(newInputs, logicalUnion.all);
            call.transformTo(newUnion);
        }
    }

    private static class PushProjectFilterLogicalViewRule extends PushFilterRule {

        public PushProjectFilterLogicalViewRule(RelOptRuleOperand operand, String description) {
            super(operand, description);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final LogicalView logicalView = call.rel(2);
            if (logicalView instanceof OSSTableScan) {
                return false;
            }
            return true;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Project project = call.rel(0);
            Filter filter = call.rel(1);
            LogicalView logicalView = call.rel(2);

            if (PushFilterRule.doNotPush(filter, logicalView)) {
                return;
            }

            Filter newLogicalFilter =
                filter.copy(filter.getTraitSet(), filter.getInput(), filter.getCondition());

            if (PushProjectRule.doNotPush(project)) {
                // make convention none to match push (semi)join rule
                LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(Convention.NONE));
                newLogicalView.push(newLogicalFilter);

                Project newProject = project
                    .copy(project.getTraitSet(), newLogicalView, project.getProjects(),
                        project.getRowType());
                call.transformTo(newProject);
            } else {
                // make convention none to match push (semi)join rule
                LogicalView newLogicalView = logicalView.copy(project.getTraitSet().replace(Convention.NONE));
                newLogicalView.push(newLogicalFilter);

                Project newProject = project
                    .copy(project.getTraitSet(), project.getInput(), project.getProjects(),
                        project.getRowType());
                newLogicalView.push(newProject);
                call.transformTo(newLogicalView);
            }
        }
    }

    private static class PushFilterVirtualViewRule extends PushFilterRule {

        public PushFilterVirtualViewRule(RelOptRuleOperand operand, String desc) {
            super(operand, desc);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            VirtualView virtualView = call.rel(1);
            virtualView.pushFilter(filter.getCondition());
        }
    }

}
