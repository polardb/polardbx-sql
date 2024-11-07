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
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.polardbx.common.properties.ConnectionParams.DML_FORBID_PUSH_DOWN_UPDATE_WITH_SUBQUERY_IN_SET;
import static com.alibaba.polardbx.common.properties.ConnectionParams.DML_PUSH_MODIFY_WITH_SUBQUERY_CONDITION_OF_TARGET;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyBroadcast;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyFkReferenced;
import static com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation.checkModifyFkReferencing;
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
    public static final PushModifyRule OPTIMIZE_MODIFY_TOP_N_RULE = new OptimizeModifyTopNRule();

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

        ExecutionContext ec = context.getExecutionContext();

        boolean containsUpdateFks =
            modify.isUpdate() && (checkModifyFkReferenced(modify, context.getExecutionContext())
                || checkModifyFkReferencing(modify, context.getExecutionContext()));
        boolean containsDeleteFks =
            modify.isDelete() && (checkModifyFkReferenced(modify, context.getExecutionContext())
                || checkModifyFkReferencing(modify, context.getExecutionContext()));

        if (modifyBroadcastTable || checkModifyGsi(modify, context.getExecutionContext()) || modifyScaleoutTable ||
            CheckModifyLimitation.checkHasLogicalGeneratedColumns(modify, context.getExecutionContext()) ||
            (ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK) && modify.isUpdate()) ||
            (ec.foreignKeyChecks() && (containsUpdateFks || containsDeleteFks))
        ) {
            // 1. Do not pushdown multi table UPDATE/DELETE modifying broadcast table
            // 2. Do not pushdown UPDATE/DELETE if modifying gsi table
            // 3. Do not pushdown the table which is in scaleout writable phase
            // 4. Do not pushdown the table which is doing online column ddl
            // 5. Do not pushdown UPDATE if we need to check primary key
            // 6. Do not pushdown UPDATE if we need to check foreign key and
            //    the table which is referenced by or referencing foreign constraint of other table
            // 7. Do not pushdown DELETE if we need to check foreign key and
            //    the table which is referenced by foreign constraint of other table
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

            final PlannerContext context = PlannerContext.getPlannerContext(call);
            final ExecutionContext ec = context.getExecutionContext();

            if (forbidPushdownForDelete(modify, lv) || forbidPushDownForDeleteOrUpdate(modify, lv, ec)) {
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
            super(operand(LogicalModify.class, operand(MergeSort.class, operand(LogicalView.class, none()))),
                "LogicalModify_MergeSort_LogicalView");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalModify modify = (LogicalModify) call.rels[0];
            MergeSort sort = (MergeSort) call.rels[1];
            final LogicalView lv = (LogicalView) call.rels[2];
            final PlannerContext context = PlannerContext.getPlannerContext(call);
            final ExecutionContext ec = context.getExecutionContext();

            if (forbidPushdownForDelete(modify, lv) || forbidPushDownForDeleteOrUpdate(modify, lv, ec)) {
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
            final ExecutionContext ec = context.getExecutionContext();

            if (forbidPushdownForDelete(modify, lv) || forbidPushDownForDeleteOrUpdate(modify, lv, ec)) {
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

    private static class OptimizeModifyTopNRule extends PushModifyRule {

        public OptimizeModifyTopNRule() {
            super(operand(LogicalModify.class, operand(Sort.class, operand(LogicalView.class, none()))),
                "OptimizeModifyTopNRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final LogicalModify modify = call.rel(0);
            if (modify.isModifyTopN()) {
                return false;
            }
            return super.matches(call);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalModify modify = (LogicalModify) call.rels[0];
            final Sort sort = (Sort) call.rels[1];
            final LogicalView lv = (LogicalView) call.rels[2];
            final PlannerContext context = PlannerContext.getPlannerContext(call);

            // check whether modify top n can be optimized by returning
            if (sort.offset == null && sort.fetch != null) {
                // MySQL only support specify literal value for fetch clause.
                // We replace fetch clause with RexDynamicParam in DrdsParameterizeSqlVisitor.
                // So that sort.fetch must be a RexDynamicParam, just double check for sure
                final boolean parameterizedFetch = sort.fetch instanceof RexDynamicParam;
                final boolean multiTableModify = lv.getTableNames().size() > 1;
                final boolean singleGroup = lv.isSingleGroup(true);
                final boolean notNewPartitionTable = !PartitionUtils.isNewPartShardTable(lv);

                // 1. MySQL does not support multi table update/delete with limit
                // 2. No optimization needed for single group update/delete
                // 3. Only support new partition table
                boolean isModifyTopN =
                    !(multiTableModify || singleGroup || notNewPartitionTable) && parameterizedFetch;

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(modify.getTargetTables().get(0));

                // 4. At least one partition level is partition by and sorted by order by columns in LogicalView
                if (isModifyTopN) {
                    final PartitionInfo partitionInfo = context
                        .getExecutionContext()
                        .getSchemaManager(qn.left)
                        .getTable(qn.right)
                        .getPartitionInfo();

                    boolean partitionsSortedBySortKeyInLv = false;
                    final List<String> partColumnNames = new ArrayList<>();
                    if (PartitionPrunerUtils.checkPartitionsSortedByPartitionColumns(partitionInfo,
                        // check first level partition
                        PartKeyLevel.PARTITION_KEY,
                        partColumnNames)) {
                        partitionsSortedBySortKeyInLv |= PartitionUtils.isOrderKeyMatched(lv, partColumnNames);
                    }
                    partColumnNames.clear();
                    if (PartitionPrunerUtils.checkPartitionsSortedByPartitionColumns(partitionInfo,
                        // check second level partition
                        PartKeyLevel.SUBPARTITION_KEY,
                        partColumnNames)) {
                        partitionsSortedBySortKeyInLv |= PartitionUtils.isOrderKeyMatched(lv, partColumnNames);
                    }

                    isModifyTopN &= partitionsSortedBySortKeyInLv;
                }

                // mark plan can be pushdown and optimized as modify on top n
                if (isModifyTopN) {
                    final List<String> pkColumnNames =
                        GlobalIndexMeta.getPrimaryKeys(qn.right, qn.left, context.getExecutionContext());

                    modify.setModifyTopNInfo(
                        LogicalModify.ModifyTopNInfo.create(pkColumnNames,
                            sort.getChildExps(),
                            sort.collation,
                            (RexDynamicParam) sort.fetch));

                    call.transformTo(modify);
                }
            }
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

    /**
     * Update / Delete limit m,n 时，由于mysql 不支持 limit m,n; 所以禁止下推
     * Update / Delete WHERE 中包含目标表的子查询 时，由于 mysql 不支持, 禁止下推
     */
    private static boolean forbidPushDownForDeleteOrUpdate(final TableModify modify,
                                                           final LogicalView lv,
                                                           final ExecutionContext ec) {
        if (modify instanceof LogicalModify && ((LogicalModify) modify).getOriginalSqlNode() != null) {
            SqlNode originalNode = ((LogicalModify) modify).getOriginalSqlNode();
            if (originalNode instanceof SqlDelete && ((SqlDelete) originalNode).getOffset() != null) {
                return true;
            }
            if (originalNode instanceof SqlUpdate && ((SqlUpdate) originalNode).getOffset() != null) {
                return true;
            }
        }

        if (!ec.getParamManager().getBoolean(DML_PUSH_MODIFY_WITH_SUBQUERY_CONDITION_OF_TARGET)) {
            final List<String> pushdownTableNames = lv.getTableNames();
            final Map<String, Integer> tableCountMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (String tn : pushdownTableNames) {
                tableCountMap.compute(tn, (k, v) -> v == null ? 1 : v + 1);
            }
            for (RelOptTable t : modify.getTargetTables()) {
                if (tableCountMap.get(RelUtils.getQualifiedTableName(t).right) > 1) {
                    return true;
                }
            }
        }

        if (ec.getParamManager().getBoolean(DML_FORBID_PUSH_DOWN_UPDATE_WITH_SUBQUERY_IN_SET)) {
            return modify.getSourceExpressionList() != null && modify.getSourceExpressionList().stream()
                .anyMatch(RexUtil::hasSubQuery);
        }

        return false;
    }

}
