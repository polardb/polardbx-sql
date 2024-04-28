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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BuildFinalPlanVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.util.Pair;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.utils.BuildPlanUtils.buildUpdateColumnList;
import static com.alibaba.polardbx.optimizer.utils.BuildPlanUtils.checkAllUpdatedSkRefAfterValue;
import static com.alibaba.polardbx.optimizer.utils.BuildPlanUtils.checkAllUpdatedSkRefBeforeValue;

/**
 * @author chenmo.cm
 */
public enum ExecutionStrategy {
    /**
     * Foreach row, exists only one target partition.
     * Pushdown origin statement, with function call not pushable (like sequence call) replaced by RexCallParam.
     * Typical for single table and partitioned table without gsi.
     */
    PUSHDOWN,
    /**
     * Foreach row, might exists more than one target partition.
     * Pushdown origin statement, with nondeterministic function call replaced by RexCallParam.
     * Typical for broadcast table.
     */
    DETERMINISTIC_PUSHDOWN,
    /**
     * Foreach row, might exists more than one target partition, and data in different target partitions might be different.
     * Select then execute, with all function call replaced by RexCallParam.
     * Typical for table with gsi or table are doing scale out.
     */
    LOGICAL;

    public static ExecutionStrategy fromValue(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        try {
            return ExecutionStrategy.valueOf(value.toUpperCase());
        } catch (Exception e) {
            return null;
        }
    }

    public static ExecutionStrategyResult determineExecutionStrategy(LogicalInsert insert, PlannerContext context) {

        ExecutionStrategyResult result = new ExecutionStrategyResult();
        final ExecutionContext ec = context.getExecutionContext();
        final ExecutionStrategy strategy = fromHint(ec);
        if (strategy != null) {
            result.useStrategyByHintParams = true;
            result.execStrategy = strategy;
            return result;
        }

        final boolean foreignKeyChecks = context.getExecutionContext().foreignKeyChecks();
        final boolean pushableFkCheck = pushableForeignConstraintCheck(insert, context);

        final String schema = insert.getSchemaName();
        final String targetTable = insert.getLogicalTableName();

        final OptimizerContext oc = OptimizerContext.getContext(schema);
        assert null != oc;

        // Table detail
        final boolean isBroadcast = oc.getRuleManager().isBroadCast(targetTable);
        final boolean isSingleTable = oc.getRuleManager().isTableInSingleDb(targetTable);
        final boolean isPartitioned = oc.getRuleManager().isShard(targetTable);

        // Gsi detail
        final RelOptTable primaryTable = insert.getTable();
        final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(primaryTable, ec);
        final boolean withGsi = !gsiMetas.isEmpty();
        final boolean allGsiPublished = GlobalIndexMeta.isAllGsiPublished(gsiMetas, context);

        final boolean onlineModifyColumn = TableColumnUtils.isModifying(primaryTable, ec);

        // Unique key detail
        final List<List<String>> uniqueKeys = GlobalIndexMeta.getUniqueKeys(targetTable, schema, true, tm -> true, ec);
        final boolean withoutPkAndUk = uniqueKeys.isEmpty() || uniqueKeys.get(0).isEmpty();

        // Scale out task detail
        final TableMeta primaryTableMeta = context.getExecutionContext().getSchemaManager(schema).getTable(targetTable);

        // replicate task detail
        final boolean replicateCanWrite = ComplexTaskPlanUtils.canWrite(primaryTableMeta);
        final boolean replicateDeleteOnly = ComplexTaskPlanUtils.isDeleteOnly(primaryTableMeta);
        final boolean replicateReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(primaryTableMeta);
        final boolean replicateIsRunning = replicateCanWrite;
        final boolean replicateConsistentBaseData = !replicateCanWrite || replicateReadyToPublish;

        // Get relation between uk and partition key
        final TreeSet<String> partitionKeys = GlobalIndexMeta.getPartitionKeySet(targetTable, schema, ec);
        final boolean ukContainsPartitionKey =
            GlobalIndexMeta.isEveryUkContainsAllPartitionKey(targetTable, schema, true, ec);

        // Statement detail
        final boolean canPushDuplicateCheck =
            withoutPkAndUk || (ukContainsPartitionKey && allGsiPublished
                && replicateConsistentBaseData);
        final boolean canPushDuplicateIgnoreScaleOutCheck =
            withoutPkAndUk || (ukContainsPartitionKey && allGsiPublished);
        final boolean defaultPushDuplicateCheck =
            Optional.ofNullable(ec).map(e -> e.getParamManager().getBoolean(ConnectionParams.DML_PUSH_DUPLICATE_CHECK))
                .orElseGet(() -> Boolean.valueOf(ConnectionParams.DML_PUSH_DUPLICATE_CHECK.getDefault()));

        final boolean pushDuplicateCheck =
            !withGsi && !replicateIsRunning && defaultPushDuplicateCheck;

        final boolean primaryKeyCheck = ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK);
        final boolean pushablePrimaryKeyCheck = pushablePrimaryKeyConstraint(context, schema, targetTable);

        boolean multiWriteForReplication;
        boolean canPush;
        if (insert.isUpsert()) {
            final List<String> updateCols = buildUpdateColumnList(insert);

            // If all sharding columns in update list referencing same column in after value
            // e.g. insert into t1(a,b,c) values (1,2,3) on duplicate key update a=values(a),b=values(b),c=values(c)
            final boolean allUpdatedSkRefValue = checkAllUpdatedSkRefAfterValue(updateCols, partitionKeys, insert)
                || checkAllUpdatedSkRefBeforeValue(updateCols, partitionKeys, insert);

            final boolean modifyPartitionKey =
                updateCols.stream().anyMatch(partitionKeys::contains) && !withoutPkAndUk && !allUpdatedSkRefValue;

            final Set<String> gsiCol = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            gsiMetas.stream().flatMap(tm -> tm.getAllColumns().stream()).map(ColumnMeta::getName).forEach(gsiCol::add);
            final boolean updateGsi = updateCols.stream().anyMatch(gsiCol::contains) && !withoutPkAndUk;

            // For UPSERT, do DELETE when DELETE_ONLY, do UPSERT when WRITE_ONLY
            multiWriteForReplication = replicateCanWrite;
            boolean containUnpushableFunction = insert.getDuplicateKeyUpdateValueList().stream()
                .anyMatch(rex -> !BuildFinalPlanVisitor.canBePushDown(rex, multiWriteForReplication));

            canPush = (pushDuplicateCheck || canPushDuplicateCheck) && !modifyPartitionKey && !updateGsi
                && !containUnpushableFunction;
            canPush = canPush && (!foreignKeyChecks || !CheckModifyLimitation.checkModifyFkReferenced(insert,
                context.getExecutionContext()));
        } else if (insert.isReplace()) {
            // For REPLACE, do DELETE when DELETE_ONLY, do REPLACE when WRITE_ONLY
            multiWriteForReplication = replicateCanWrite;
            canPush = pushDuplicateCheck || canPushDuplicateCheck;
            canPush = canPush && (!foreignKeyChecks || !CheckModifyLimitation.checkModifyFkReferenced(insert,
                context.getExecutionContext()));
        } else if (insert.isInsertIgnore()) {
            // For INSERT IGNORE, DO NOT push down it even for DELETE_ONLY, because the table meta could change when reload
            multiWriteForReplication = replicateCanWrite;
            canPush = pushDuplicateCheck || canPushDuplicateCheck;
        } else {
            // For INSERT, do nothing when DELETE_ONLY, do INSERT when WRITE_ONLY
            multiWriteForReplication = (replicateCanWrite && !replicateDeleteOnly);
            canPush = true;
        }

        final boolean doMultiWrite = isBroadcast || withGsi || multiWriteForReplication;
        // Do not push down when modify column for now
        canPush = canPush && !onlineModifyColumn;
        canPush = canPush && (!primaryKeyCheck || pushablePrimaryKeyCheck);
        canPush = canPush && (!foreignKeyChecks || pushableFkCheck);

        result.pushDuplicateCheckByHintParams = pushDuplicateCheck;
        result.canPushDuplicateIgnoreScaleOutCheck = canPushDuplicateIgnoreScaleOutCheck;
        result.doMultiWrite = doMultiWrite;
        result.pushablePrimaryKeyCheck = pushablePrimaryKeyCheck;
        result.pushableForeignConstraintCheck = pushableFkCheck;

        // Pushdown dml on single/partition table without replica for performance
        if (!doMultiWrite && canPush) {
            result.execStrategy = PUSHDOWN;
            return result;
        }

        // For table with multi-replica, we need deterministic pushdown which will replace non-deterministic function call with literal
        if (canPush) {
            result.execStrategy = DETERMINISTIC_PUSHDOWN;
            return result;
        }

        result.execStrategy = LOGICAL;
        return result;
    }

    public static boolean pushablePrimaryKeyConstraint(PlannerContext context, String schemaName, String tableName) {
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        final TddlRuleManager rm = oc.getRuleManager();
        final boolean isBroadcast = rm.isBroadCast(tableName);
        final boolean isSingleTable = rm.isTableInSingleDb(tableName);
        if (isBroadcast || isSingleTable) {
            return true;
        }

        final ExecutionContext ec = context.getExecutionContext();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        final List<String> partitionKey = rm.getSharedColumns(tableMeta.getTableName());
        final TreeSet<String> primaryKeySet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        primaryKeySet.addAll(tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        return primaryKeySet.containsAll(partitionKey);
    }

    public static ExecutionStrategy fromHint(ExecutionContext ec) {
        // Using HINT /*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=XXX)*/
        return Optional.ofNullable(ec).map(
                e -> ExecutionStrategy.fromValue(ec.getParamManager().getString(ConnectionParams.DML_EXECUTION_STRATEGY)))
            .orElse(null);
    }

    public static EnumSet<ExecutionStrategy> REPLACE_NON_DETERMINISTIC_FUNCTION =
        EnumSet.of(LOGICAL, DETERMINISTIC_PUSHDOWN);

    public boolean replaceNonDeterministicFunction() {
        return REPLACE_NON_DETERMINISTIC_FUNCTION.contains(this);
    }

    public static boolean pushableForeignConstraintCheck(LogicalInsert insert, PlannerContext context) {
        final String schema = insert.getSchemaName();
        final String targetTable = insert.getLogicalTableName();
        final TableMeta tableMeta = context.getExecutionContext().getSchemaManager(schema).getTable(targetTable);

        final List<Pair<String, ForeignKeyData>> refTables =
            tableMeta.getForeignKeys().values().stream().map(v -> Pair.of(v.refTableName, v))
                .collect(Collectors.toList());

        return refTables.stream()
            .allMatch(refTable -> pushableForeignConstraint(context, refTable.right.refSchema, targetTable, refTable));
    }

    public static boolean pushableForeignConstraint(PlannerContext context, String schema,
                                                    String targetTable,
                                                    Pair<String, ForeignKeyData> refTable,
                                                    SqlCreateTable sqlCreateTable) {
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);
        final boolean isRefNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(refTable.right.refSchema);

        if (isNewPartDb != isRefNewPartDb) {
            return false;
        }

        final TddlRuleManager or =
            context.getExecutionContext().getSchemaManager(schema).getTddlRuleManager();

        if (isNewPartDb) {
            // check after table group id calculated
            return true;
        } else {
            if (targetTable.equals(refTable.left)) {
                return sqlCreateTable.isSingle() || sqlCreateTable.isBroadCast();
            }

            if (or.getTableRule(refTable.left) == null) {
                return false;
            }

            if (sqlCreateTable.isSingle() && or.isTableInSingleDb(refTable.left) ||
                sqlCreateTable.isBroadCast() && or.isBroadCast(refTable.left)) {
                return true;
            }

            return false;
        }
    }

    public static boolean pushableForeignConstraint(PlannerContext context, String schema, String targetTable,
                                                    Pair<String, ForeignKeyData> refTable) {
        final boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schema);

        final TddlRuleManager orLeft =
            context.getExecutionContext().getSchemaManager(refTable.right.schema).getTddlRuleManager();

        final TddlRuleManager orRight =
            context.getExecutionContext().getSchemaManager(schema).getTddlRuleManager();

        if (isNewPartDb) {
            final PartitionInfo leftPartitionInfo =
                orLeft.getPartitionInfoManager().getPartitionInfo(targetTable);
            final PartitionInfo rightPartitionInfo =
                orRight.getPartitionInfoManager().getPartitionInfo(refTable.left);

            if (leftPartitionInfo.isBroadcastTable() && rightPartitionInfo.isBroadcastTable()) {
                return true;
            }

            if (leftPartitionInfo.getTableGroupId() == null || rightPartitionInfo == null
                || rightPartitionInfo.getTableGroupId() == null) {
                return false;
            } else if (!leftPartitionInfo.getTableGroupId().equals(rightPartitionInfo.getTableGroupId())) {
                return false;
            }

            if (leftPartitionInfo.isSingleTable() && rightPartitionInfo.isSingleTable()) {
                return true;
            }

            return false;

        } else {
            if (orLeft.getTableRule(refTable.left) == null) {
                return false;
            }

            if (orRight.isTableInSingleDb(targetTable) && orLeft.isTableInSingleDb(refTable.left) ||
                orRight.isBroadCast(targetTable) && orLeft.isBroadCast(refTable.left)) {
                return true;
            }

            return false;
        }
    }

    public static boolean isPushableFkReference(List<String> tarCols, List<String> refCols,
                                                List<String> lPartitionCols, List<String> rPartitionCols) {
        for (int i = 0; i < lPartitionCols.size(); i++) {
            final String lPartitionCol = lPartitionCols.get(i);
            final String rPartitionCol = rPartitionCols.get(i);

            boolean matched = false;
            for (int j = 0; j < tarCols.size(); j++) {
                if (lPartitionCol.equalsIgnoreCase(tarCols.get(j))) {
                    if (!rPartitionCol.equalsIgnoreCase(refCols.get(j))) {
                        return false;
                    } else {
                        matched = true;
                        break;
                    }
                }
            }

            if (!matched) {
                return false;
            }
        }
        return true;
    }
}
