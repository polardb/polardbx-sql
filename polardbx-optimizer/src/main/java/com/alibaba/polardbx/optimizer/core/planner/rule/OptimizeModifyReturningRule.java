/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify.LogicalMultiWriteInfo;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils.LogicalModifyViewBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyGsi;

public abstract class OptimizeModifyReturningRule extends RelOptRule implements LogicalModifyViewBuilder {

    public static final OptimizeModifyReturningRule MODIFY_VIEW = new OptimizeByReturningModifyViewRule();
    public static final OptimizeModifyReturningRule MODIFY_SORT_VIEW = new OptimizeByReturningSortRule();
    public static final OptimizeModifyReturningRule MODIFY_MERGESORT_VIEW = new OptimizeByReturningMergeSortRule();

    public static final List<OptimizeModifyReturningRule> OPTIMIZE_MODIFY_RETURNING_RULES = ImmutableList.of(
        MODIFY_VIEW,
        MODIFY_SORT_VIEW,
        MODIFY_MERGESORT_VIEW
    );

    public OptimizeModifyReturningRule(RelOptRuleOperand operand, String description) {
        super(operand, "OptimizeModifyReturningRule:" + description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalModify modify = call.rel(0);
        if (modify.isMultiWriteCanBeOptimizedByReturning()) {
            return false;
        }

        final PlannerContext context = PlannerContext.getPlannerContext(call);
        // TODO support optimize modify on broadcast with returning
        final boolean modifyGsi = checkModifyGsi(modify, context.getExecutionContext());
        final boolean singleTableModify = (modify.getPrimaryModifyWriters().size() == 1);

        return modify.isDelete() && singleTableModify && modifyGsi;
    }

    private static class OptimizeByReturningModifyViewRule extends OptimizeModifyReturningRule {

        public OptimizeByReturningModifyViewRule() {
            super(operand(LogicalModify.class, operand(LogicalView.class, none())), "LogicalModify_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalModify modify = call.rel(0);

            final LogicalMultiWriteInfo multiWriteInfo = LogicalMultiWriteInfo
                .create(false, false)
                .setLmvBuilder(this)
                .setOptimizeByReturning(true);
            modify.setMultiWriteInfo(multiWriteInfo);
            call.transformTo(modify);
        }

        @Override
        public LogicalModifyView buildForPrimary(List<RelNode> bindings) {
            final LogicalModify modify = (LogicalModify) bindings.get(0);
            final LogicalView lv = (LogicalView) bindings.get(1);

            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());

            return lmv;
        }

        @Override
        public List<RelNode> bindPlan(LogicalModify modify) {
            final List<RelNode> bindings = new ArrayList<>();
            // LogicalModify-LogicalView
            if (!RelUtils.matchPlan(modify, getOperand(), bindings, null)) {
                bindings.clear();
                // LogicalModify-Gather-LogicalView
                if (RelUtils.matchPlan(modify,
                    operand(LogicalModify.class,
                        operand(Gather.class,
                            operand(LogicalView.class, none()))),
                    bindings, null)) {
                    // Remove Gather
                    bindings.set(1, bindings.get(2));
                    bindings.remove(2);
                } else {
                    // Miss match
                    return new ArrayList<>();
                }
            }

            return bindings;
        }
    }

    private static @NotNull LogicalMultiWriteInfo checkAndBuildMultiWriteInfo(Sort sort, LogicalView lv,
                                                                              LogicalModifyViewBuilder lmvBuilder) {
        final LogicalMultiWriteInfo multiWriteInfo = LogicalMultiWriteInfo
            .create(sort.offset != null, sort.fetch != null);

        // Following cases can use returning no matter how many partitions left after partition pruning
        // 1. DELETE with order but no offset and fetch
        // 2. LogicalView is single group (with partition hint or equal condition on partition key)
        final boolean singleGroup = lv.isSingleGroup(false);
        boolean canUseReturning = !sort.withLimit() || singleGroup;
        if (canUseReturning) {
            if (singleGroup) {
                multiWriteInfo.setLmvBuilder(MODIFY_VIEW);
            } else {
                multiWriteInfo.setLmvBuilder(lmvBuilder);
            }
            multiWriteInfo.setOptimizeByReturning(true);
        }
        return multiWriteInfo;
    }

    private static class OptimizeByReturningMergeSortRule extends OptimizeModifyReturningRule {

        public OptimizeByReturningMergeSortRule() {
            super(operand(LogicalModify.class, operand(MergeSort.class, operand(LogicalView.class, none()))),
                "LogicalModify_MergeSort_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalModify modify = call.rel(0);
            final MergeSort sort = (MergeSort) call.rels[1];
            final LogicalView lv = (LogicalView) call.rels[2];

            final LogicalMultiWriteInfo multiWriteInfo = checkAndBuildMultiWriteInfo(sort, lv, this);

            modify.setMultiWriteInfo(multiWriteInfo);
            call.transformTo(modify);
        }

        @Override
        public LogicalModifyView buildForPrimary(List<RelNode> bindings) {
            final LogicalModify modify = (LogicalModify) bindings.get(0);
            final MergeSort sort = (MergeSort) bindings.get(1);
            final LogicalView lv = (LogicalView) bindings.get(2);

            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());

            return lmv;
        }

        @Override
        public List<RelNode> bindPlan(LogicalModify modify) {
            final List<RelNode> bindings = new ArrayList<>();
            if (RelUtils.matchPlan(modify, getOperand(), bindings, null)) {
                return bindings;
            }
            // Miss match
            return new ArrayList<>();
        }
    }

    private static class OptimizeByReturningSortRule extends OptimizeModifyReturningRule {

        public OptimizeByReturningSortRule() {
            super(operand(LogicalModify.class, operand(LogicalSort.class, operand(LogicalView.class, none()))),
                "LogicalModify_LogicalSort_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalModify modify = call.rel(0);
            final LogicalSort sort = (LogicalSort) call.rels[1];
            final LogicalView lv = (LogicalView) call.rels[2];

            final LogicalMultiWriteInfo multiWriteInfo = checkAndBuildMultiWriteInfo(sort, lv, MODIFY_MERGESORT_VIEW);

            modify.setMultiWriteInfo(multiWriteInfo);
            call.transformTo(modify);
        }

        @Override
        public LogicalModifyView buildForPrimary(List<RelNode> bindings) {
            final LogicalModify modify = (LogicalModify) bindings.get(0);
            final LogicalSort sort = (LogicalSort) bindings.get(1);
            final LogicalView lv = (LogicalView) bindings.get(2);

            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());

            return lmv;
        }

        @Override
        public List<RelNode> bindPlan(LogicalModify modify) {
            final List<RelNode> bindings = new ArrayList<>();
            if (RelUtils.matchPlan(modify, getOperand(), bindings, null)) {
                return bindings;
            }
            // Miss match
            return new ArrayList<>();
        }
    }
}
