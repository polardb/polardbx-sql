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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class UpdateDeleteIndexExecutor extends IndexExecutor {

    public UpdateDeleteIndexExecutor(BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                     String schemaName) {
        super(executeFunc, schemaName);
    }

    /**
     * TODO: Gather all sql in the same group, and send them together to MySQL,
     * even if they are updating different indexes.
     *
     * @param basePlan logical SqlUpdate or SqlDelete
     * @param physicalPlans physical SqlUpdate or SqlDelete on base table
     */
    public int execute(String baseTableName, SqlNode basePlan, List<RelNode> physicalPlans,
                       ExecutionContext executionContext) {

        TableMeta baseTableMeta = executionContext.getSchemaManager(schemaName).getTable(baseTableName);
        List<TableMeta> indexMetas = GlobalIndexMeta.getIndex(baseTableName, schemaName, executionContext);

        checkUpdatingShardingKey(basePlan, baseTableMeta, indexMetas);
        checkUniqueKeyNull(basePlan, baseTableMeta, executionContext);

        // query the data to be updated or deleted
        List<String> selectKeys = GlobalIndexMeta.getPrimaryAndShardingKeys(baseTableMeta, indexMetas, schemaName);
        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
        List<RelNode> selects = builder.buildSelect(baseTableMeta, physicalPlans, selectKeys);

        List<List<Object>> results = getSelectResults(selects, executionContext);
        if (results.isEmpty()) {
            return 0;
        }

        PhyTableOperation oneOldPhysicalPlan = (PhyTableOperation) physicalPlans.get(0);
        int affectRowsInBase = executeOnTable(basePlan,
            baseTableMeta,
            executionContext,
            results,
            selectKeys,
            oneOldPhysicalPlan);

        // update index tables
        for (TableMeta indexMeta : indexMetas) {
            // Don't compare affect rows, because affect rows in index may be
            // less than that of the base
            executeOnTable(basePlan, indexMeta, executionContext, results, selectKeys, oneOldPhysicalPlan);
        }

        return affectRowsInBase;
    }

    /**
     * Check if sharding keys are to be updated.
     */
    private void checkUpdatingShardingKey(SqlNode basePlan, TableMeta baseTableMeta, List<TableMeta> indexMetas) {
        if (basePlan.getKind() != SqlKind.UPDATE) {
            return;
        }

        List<TableMeta> allTables = new ArrayList<>(indexMetas.size() + 1);
        allTables.add(baseTableMeta);
        allTables.addAll(indexMetas);

        // column names are already in upper case
        Map<String, String> keyToTable = new HashMap<>();
        for (TableMeta tableMeta : allTables) {
            List<String> shardingKeys = GlobalIndexMeta.getShardingKeys(tableMeta, schemaName);
            for (String key : shardingKeys) {
                keyToTable.putIfAbsent(key, tableMeta.getTableName());
            }
        }

        SqlNodeList columnList = ((SqlUpdate) basePlan).getTargetColumnList();
        for (SqlNode column : columnList) {
            String columnName = ((SqlIdentifier) column).getLastName().toUpperCase();
            String tableName = keyToTable.get(columnName);
            if (tableName != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_SHARD_COLUMN, columnName, tableName);
            }
        }
    }

    /**
     * Check if unique key is NULL.
     */
    private void checkUniqueKeyNull(SqlNode basePlan, TableMeta tableMeta, ExecutionContext executionContext) {
        if (basePlan.getKind() != SqlKind.UPDATE) {
            return;
        }

        List<String> uniqueKeys =
            GlobalIndexMeta
                .getUniqueKeyColumnList(tableMeta.getTableName(), tableMeta.getSchemaName(), false, executionContext);
        if (uniqueKeys.isEmpty()) {
            return;
        }

        // uniqueKeys are already in upper case
        SqlUpdate sqlUpdate = (SqlUpdate) basePlan;
        List<String> columnList = sqlUpdate.getTargetColumnList()
            .getList()
            .stream()
            .map(sqlNode -> ((SqlIdentifier) sqlNode).getLastName().toUpperCase())
            .collect(Collectors.toList());
        List<Integer> uniqueKeyIndexes = uniqueKeys.stream().map(columnList::indexOf).collect(Collectors.toList());

        SqlNodeList expressionList = sqlUpdate.getSourceExpressionList();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        for (int i = 0; i < uniqueKeyIndexes.size(); i++) {
            int index = uniqueKeyIndexes.get(i);
            if (index < 0) {
                continue;
            }

            SqlNode expression = expressionList.get(index);
            boolean isNull = false;
            if (expression instanceof SqlDynamicParam) {
                int dynamicParamIndex = ((SqlDynamicParam) expression).getIndex();
                Object object = params.get(dynamicParamIndex + 1).getValue();
                if (object == null) {
                    isNull = true;
                }
            } else if (expression instanceof SqlLiteral) {
                Object object = ((SqlLiteral) expression).getValue();
                if (object == null) {
                    isNull = true;
                }
            } else {
                // what if function?
            }

            if (isNull) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL,
                    uniqueKeys.get(i));
            }
        }
    }

    /**
     * Execute UPDATE or DELETE on one table.
     *
     * @param basePlan logical plan, containing all rows
     * @param tableMeta base table or index table
     * @param selectedResults data that to be updated or deleted
     * @param selectKeys columns corresponding to selectedResults
     * @param oneOldPhysicalPlan any physical plan, used to create new ones
     * @return affected rows
     */
    private int executeOnTable(SqlNode basePlan, TableMeta tableMeta, ExecutionContext executionContext,
                               List<List<Object>> selectedResults, List<String> selectKeys,
                               PhyTableOperation oneOldPhysicalPlan) {
        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);

        List<RelNode> phyTableOperations;
        if (basePlan.getKind() == SqlKind.UPDATE) {
            if (tableMeta.isGsi() && !GlobalIndexMeta.canWrite(executionContext, tableMeta)) {
                if (GlobalIndexMeta.canDelete(executionContext, tableMeta)) {
                    // Delete only.
                    phyTableOperations = builder.buildDelete(tableMeta,
                        selectedResults,
                        selectKeys,
                        oneOldPhysicalPlan);
                } else {
                    return 0; // Status which update is not allowed.
                }
            } else {
                phyTableOperations = builder.buildUpdate((SqlUpdate) basePlan,
                    tableMeta,
                    selectedResults,
                    selectKeys,
                    oneOldPhysicalPlan);
            }
        } else {
            if (tableMeta.isGsi() && !GlobalIndexMeta.canDelete(executionContext, tableMeta)) {
                return 0; // Status which deletion is not allowed.
            }

            phyTableOperations = builder.buildDelete(tableMeta,
                selectedResults,
                selectKeys,
                oneOldPhysicalPlan);
        }

        // If none of the SET columns in the UPDATE are in the index, pass
        if (!phyTableOperations.isEmpty()) {
            List<Cursor> cursors = executeFunc.apply(phyTableOperations, executionContext);
            return ExecUtils.getAffectRowsByCursors(cursors, false);
        } else {
            return 0;
        }
    }
}
