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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.commons.lang3.StringUtils;

import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyBroadcast;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyGsi;

/**
 * @author lingce.ldm 2018-01-30 19:36
 */
public abstract class PushModifyRule extends RelOptRule {

    public static Logger logger = LoggerFactory.getLogger(PushModifyRule.class);

    public PushModifyRule(RelOptRuleOperand operand, String description) {
        super(operand, "Push_down_rule:" + description);
    }

    public static final PushModifyRule VIEW = new PushModifyViewRule();
    public static final PushModifyRule MERGESORT = new PushModifyMergeSortRule();
    public static final PushModifyRule SORT_VIEW = new PushModifySortRule();

    @Override
    public boolean matches(RelOptRuleCall call) {
        final TableModify modify = call.rel(0);
        if (modify.isInsert() || modify.isReplace() || modify instanceof LogicalRelocate) {
            return false;
        }
        final PlannerContext context = PlannerContext.getPlannerContext(call);

        final boolean modifyBroadcastTable = checkModifyBroadcast(modify, () -> {
        });
        final boolean modifyScaleoutTable = !CheckModifyLimitation
            .isAllTablesCouldPushDown(modify, context.getExecutionContext());

        if (modifyBroadcastTable || checkModifyGsi(modify, context.getExecutionContext()) || modifyScaleoutTable) {
            // 1. Do not pushdown multi table UPDATE/DELETE modifying broadcast table
            // 2. Do not pushdown UPDATE/DELETE if modifying gsi table
            // 3. Do not pushdown the table which is in scaleout writable phase
            return false;
        }

        final ExecutionStrategy strategy = ExecutionStrategy.fromHint(context.getExecutionContext());
        if (ExecutionStrategy.LOGICAL == strategy) {
            return false;
        }
        return super.matches(call);
    }

    private static class PushModifyViewRule extends PushModifyRule {

        public PushModifyViewRule() {
            super(operand(TableModify.class, operand(LogicalView.class, none())), "TableModify_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableModify modify = (TableModify) call.rels[0];
            LogicalView lv = (LogicalView) call.rels[1];

            if (forbidPushdownForDelete(modify, lv)) {
                return;
            }

            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.setHintContext(modify.getHintContext());
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());
            call.transformTo(lmv);
        }
    }

    private static class PushModifyMergeSortRule extends PushModifyRule {

        public PushModifyMergeSortRule() {
            super(operand(TableModify.class, operand(MergeSort.class, operand(LogicalView.class, none()))),
                "TableModify_MergeSort_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableModify modify = (TableModify) call.rels[0];
            MergeSort sort = (MergeSort) call.rels[1];
            final LogicalView lv = (LogicalView) call.rels[2];
            final PlannerContext context = PlannerContext.getPlannerContext(call);

            if (forbidPushdownForDelete(modify, lv)) {
                return;
            }

            /**
             * For DML, DO NOT support limit with more than one physical table.
             */
            if (sort.fetch != null) {
                if (!lv.isSingleGroup(true)
                    && !context.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                    throw new TddlRuntimeException(ErrorCode.ERROR_MERGE_UPDATE_WITH_LIMIT);
                } else {
                    // for merge update with limit, throw exception in post planner
                    return;
                }
            }

            /**
             * Do not have fetch, remove the mergeSort.
             */
            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());
            call.transformTo(lmv);
        }
    }

    private static class PushModifySortRule extends PushModifyRule {

        public PushModifySortRule() {
            super(operand(TableModify.class, operand(LogicalSort.class, operand(LogicalView.class, none()))),
                "TableModify_Sort_VIEW");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableModify modify = (TableModify) call.rels[0];
            LogicalSort sort = (LogicalSort) call.rels[1];
            final LogicalView lv = (LogicalView) call.rels[2];
            final PlannerContext context = PlannerContext.getPlannerContext(call);

            if (forbidPushdownForDelete(modify, lv)) {
                return;
            }

            /**
             * For DML, DO NOT support limit with more than one physical table.
             */
            if (sort.fetch != null) {
                if (!lv.isSingleGroup(true)
                    && !context.getParamManager().getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                    throw new TddlRuntimeException(ErrorCode.ERROR_MERGE_UPDATE_WITH_LIMIT);
                } else {
                    // for merge update with limit, throw exception in post planner
                    return;
                }
            }

            /**
             * Do not have fetch, remove the mergeSort.
             */
            LogicalModifyView lmv = new LogicalModifyView(lv);
            lmv.push(modify);
            RelUtils.changeRowType(lmv, modify.getRowType());
            call.transformTo(lmv);
        }
    }

    /**
     * if there's a correlated subquery in delete, forbid pushing down to DN
     */
    private static boolean forbidPushdownForDelete(final TableModify modify, LogicalView lv) {
        try {
            if (modify == null || lv == null || !modify.isDelete()) {
                return false;
            }
            final String schemaName = modify.getSchemaName();
            boolean allLogicalNameEqualsPhysicalName = true;
            for (String logicalTableName : modify.getTargetTableNames()) {
                PartitionInfoManager partitionInfoManager =
                    PlannerContext.getPlannerContext(lv).getExecutionContext().getSchemaManager(schemaName)
                        .getTddlRuleManager()
                        .getPartitionInfoManager();
                if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
                    allLogicalNameEqualsPhysicalName = false;
                    continue;
                }
                TableRule tr = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
                if (tr == null) {
                    return false;
                }
                if (!StringUtils.equalsIgnoreCase(logicalTableName, tr.getTbNamePattern())) {
                    allLogicalNameEqualsPhysicalName = false;
                }
            }
            boolean hasSubQuery = OptimizerUtils.findRexSubquery(lv.getPushedRelNode());
            return hasSubQuery && !allLogicalNameEqualsPhysicalName;
        } catch (Exception e) {
            logger.error("unexpected exception while trying to forbidPushdownForDelete", e);
            return true;
        }
    }

}
