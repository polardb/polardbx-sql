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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import org.apache.calcite.plan.RelOptTable;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.optimizer.utils.BuildPlanUtils.buildUpdateColumnList;

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

        if (isSingleTable && !withGsi) {
            result.execStrategy = PUSHDOWN;
            return result;
        }

        // Unique key detail
        final List<List<String>> uniqueKeys = GlobalIndexMeta.getUniqueKeys(targetTable, schema, ec);
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
            GlobalIndexMeta.isEveryUkContainsPartitionKey(targetTable, schema, true, ec);

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

        boolean multiWriteForReplication;
        boolean canPush;
        if (insert.isUpsert()) {
            final List<String> updateCols = buildUpdateColumnList(insert);
            final boolean modifyPartitionKey = updateCols.stream().anyMatch(partitionKeys::contains) && !withoutPkAndUk;

            final Set<String> gsiCol = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            gsiMetas.stream().flatMap(tm -> tm.getAllColumns().stream()).map(ColumnMeta::getName).forEach(gsiCol::add);
            final boolean updateGsi = updateCols.stream().anyMatch(gsiCol::contains) && !withoutPkAndUk;

            // For UPSERT, do DELETE when DELETE_ONLY, do UPSERT when WRITE_ONLY
            multiWriteForReplication = replicateCanWrite;
            canPush = (pushDuplicateCheck || canPushDuplicateCheck) && !modifyPartitionKey && !updateGsi;
        } else if (insert.isReplace()) {
            // For REPLACE, do DELETE when DELETE_ONLY, do REPLACE when WRITE_ONLY
            multiWriteForReplication = replicateCanWrite;
            canPush = pushDuplicateCheck || canPushDuplicateCheck;
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

        result.pushDuplicateCheckByHintParams = pushDuplicateCheck;
        result.canPushDuplicateIgnoreScaleOutCheck = canPushDuplicateIgnoreScaleOutCheck;
        result.doMultiWrite = doMultiWrite;

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
}
