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
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.base.Predicate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DrdsSortJoinTransposeRule extends RelOptRule {

    public static final DrdsSortJoinTransposeRule INSTANCE = new DrdsSortJoinTransposeRule(LogicalSort.class,
        Join.class,
        RelFactories.LOGICAL_BUILDER);

    public DrdsSortJoinTransposeRule(Class<? extends Sort> sortClass, Class<? extends Join> joinClass,
                                     RelBuilderFactory relBuilderFactory) {
        super(operand(sortClass, null, new Predicate<Sort>() {
                @Override
                public boolean apply(@Nullable Sort sort) {
                    return sort.withLimit();
                }
            }, operand(joinClass, null, new Predicate<Join>() {
                @Override
                public boolean apply(@Nullable Join join) {

                    return (join instanceof LogicalJoin || join instanceof LogicalSemiJoin)
                        && (join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.RIGHT)
                        && (join.getTraitSet().simplify().getCollation().isTop()
                        && join.getTraitSet().simplify().getDistribution().isTop());
                }
            }, any())), relBuilderFactory,
            "DrdsSortJoinTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        return PlannerContext.getPlannerContext(sort).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SORT_JOIN_TRANSPOSE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        final Join join = call.rel(1);

        // We create a new sort operator on the corresponding input
        final RelNode newLeftInput;
        final RelNode newRightInput;
        final RelMetadataQuery mq = call.getMetadataQuery();
        final JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        boolean isLimitPushed = false;

        final int leftWidth = join.getLeft().getRowType().getFieldList().size();

        if (join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.INNER
            || join.getJoinType() == JoinRelType.SEMI || join.getJoinType() == JoinRelType.ANTI) {

            /*
             * if order by right input column, bail out.
             */

            for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
                if (relFieldCollation.getFieldIndex() >= leftWidth) {
                    return;
                }
            }
            RelCollation collationToPush = sort.getCollation();
            collationToPush = RelCollationTraitDef.INSTANCE.canonize(collationToPush);

            /*
             * just push sort
             */
            if (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.SEMI
                || join.getJoinType() == JoinRelType.ANTI) {
                if (isPureLimit(sort)) {
                    return;
                }
                newLeftInput = sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.ANY),
                    join.getLeft(),
                    collationToPush, null, null);
            } else if (sort.offset != null && !RelMdUtil
                .areColumnsDefinitelyUnique(mq, join.getRight(), joinInfo.rightSet())) {
                if (isPureLimit(sort)) {
                    newLeftInput =
                        sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.SINGLETON),
                            join.getLeft(), sort.getCollation(), null, CBOUtil.calPushDownFetch(sort));
                } else {
                    if (PlannerContext.getPlannerContext(sort).getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_SORT_OUTERJOIN_TRANSPOSE)) {
                        newLeftInput =
                            sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.ANY),
                                join.getLeft(),
                                collationToPush, null, CBOUtil.calPushDownFetch(sort));
                    } else {
                        newLeftInput =
                            sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.ANY),
                                join.getLeft(),
                                collationToPush, null, null);
                    }
                }
            } else {
                newLeftInput = sort.copy(sort.getTraitSet().replace(collationToPush),
                    join.getLeft(),
                    collationToPush,
                    sort.offset,
                    sort.fetch);
                isLimitPushed = true;
            }
            newRightInput = join.getRight();

        } else if (join.getJoinType() == JoinRelType.RIGHT) {
            /*
             * if order by left input column, bail out.
             */
            for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
                if (relFieldCollation.getFieldIndex() < leftWidth) {
                    return;
                }
            }
            RelCollation collationToPush = RelCollations.shift(sort.getCollation(), -leftWidth);
            collationToPush = RelCollationTraitDef.INSTANCE.canonize(collationToPush);

            if (sort.offset != null && !RelMdUtil.areColumnsDefinitelyUnique(mq, join.getLeft(), joinInfo.leftSet())) {
                if (isPureLimit(sort)) {
                    newRightInput =
                        sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.SINGLETON),
                            join.getRight(), sort.getCollation(), null, CBOUtil.calPushDownFetch(sort));
                } else {
                    if (PlannerContext.getPlannerContext(sort).getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_SORT_OUTERJOIN_TRANSPOSE)) {
                        newRightInput =
                            sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.ANY),
                                join.getRight(),
                                collationToPush, null, CBOUtil.calPushDownFetch(sort));
                    } else {
                        newRightInput =
                            sort.copy(sort.getTraitSet().replace(collationToPush).replace(RelDistributions.ANY),
                                join.getRight(),
                                collationToPush, null, null);
                    }
                }
            } else {
                newRightInput = sort.copy(sort.getTraitSet().replace(collationToPush),
                    join.getRight(),
                    collationToPush,
                    sort.offset,
                    sort.fetch);
                isLimitPushed = true;
            }
            newLeftInput = join.getLeft();
        } else {
            return;
        }

        // join must be singleton distribution
        final RelNode joinCopy =
            join.copy(join.getTraitSet().replace(sort.getCollation()).replace(RelDistributions.SINGLETON),
                join.getCondition(),
                newLeftInput,
                newRightInput,
                join.getJoinType(),
                join.isSemiJoinDone());

        if (isLimitPushed || (sort.offset == null && sort.fetch == null)) {
            call.transformTo(joinCopy);
        } else {
            Sort limit = sort.copy(sort.getTraitSet(), joinCopy, RelCollations.EMPTY, sort.offset, sort.fetch);
            call.transformTo(limit);
        }
    }

    private boolean isPureLimit(Sort sort) {
        return sort.getCollation() == null
            || sort.getCollation().getFieldCollations() == null
            || sort.getCollation().getFieldCollations().size() == 0;
    }
}
