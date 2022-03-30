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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_MODIFY_SHARD_COLUMN;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_MODIFY_SHARD_COLUMN_ON_TABLE_WITHOUT_PK;

public class CheckModifyLimitation {

    public static void check(LogicalInsert logicalModify, SqlNode sqlNode, boolean skip, PlannerContext pc) {
        if (!logicalModify.isInsert() && !logicalModify.isReplace()) {
            return;
        }

        if (skip) {
            return;
        }

        String tableName = logicalModify.getLogicalTableName();
        String schemaName = logicalModify.getSchemaName();
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();

        if (!or.isTableInSingleDb(tableName) && !or.isBroadCast(tableName)) {

            Set<String> shardColumns = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            shardColumns.addAll(or.getSharedColumns(tableName));

//            // Why use SqlInsert to validate instead of LogicalModify:
//            // RexInputRef is relative to the whole table row type, not
//            // insertRowType or input.getRowType
//            for (SqlNode column : ((SqlInsert) sqlNode).getUpdateColumnList()) {
//                String columnName = ((SqlIdentifier) column).getSimple();
//                if (shardColumns.contains(columnName)) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_SHARD_COLUMN, columnName, tableName);
//                }
//            }

            Set<String> fieldNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            List<String> fieldNames = logicalModify.getInsertRowType().getFieldNames();
            fieldNameSet.addAll(fieldNames);
            // sharding keys of base table
            for (String str : shardColumns) {
                if (!fieldNameSet.contains(str)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INSERT_CONTAINS_NO_SHARDING_KEY, tableName, str);
                }
            }

            List<TableMeta> indexTableMetas = GlobalIndexMeta.getIndex(tableName, schemaName, pc.getExecutionContext());
            if (indexTableMetas != null && !indexTableMetas.isEmpty()) {
                shardColumns.clear();
                // sharding keys of index tables
                for (TableMeta indexMeta : indexTableMetas) {
                    shardColumns.addAll(or.getSharedColumns(indexMeta.getTableName()));
                    for (String str : shardColumns) {
                        if (!fieldNameSet.contains(str)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_INSERT_CONTAINS_NO_SHARDING_KEY,
                                indexMeta.getTableName(),
                                str);
                        }
                    }
                }

                // unique keys cannot be NULL, so we'd better know its value
                TableMeta baseTableMeta = pc.getExecutionContext().getSchemaManager(schemaName)
                    .getTable(tableName);
                Set<String> uniqueKeySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                baseTableMeta.getUniqueIndexes(false).forEach(indexMeta -> indexMeta.getKeyColumns()
                    .forEach(columnMeta -> uniqueKeySet.add(columnMeta.getName())));
                // global unique indexes
                indexTableMetas.forEach(tableMeta -> tableMeta.getUniqueIndexes(false)
                    .forEach(indexMeta -> indexMeta.getKeyColumns()
                        .forEach(columnMeta -> uniqueKeySet.add(columnMeta.getName()))));
                for (String str : uniqueKeySet) {
                    if (!fieldNameSet.contains(str)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_KEY_DEFAULT, str);
                    }
                }
            }
        }
    }

    public static void check(LogicalModifyView logicalModifyView, ExecutionContext ec) {
        // update / delete join
//        List<String> tableNames = logicalModifyView.getTableNames();
//        String schemaName = logicalModifyView.getSchemaName();
//        if (tableNames.size() > 1) {
//            for (String tableName : tableNames) {
//                if (GlobalIndexMeta.hasIndex(tableName, schemaName)) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UPDATE_DELETE_MULTI_TABLE);
//                }
//            }
//        }
    }

    public static void check(LogicalTableModify modify, PlannerContext pc) {
        TableModify.Operation operation = modify.getOperation();
        if (operation == Operation.UPDATE || operation == Operation.DELETE) {
            checkUpdateDelete(modify, pc);
        } else if (operation == Operation.INSERT || operation == Operation.REPLACE) {
            checkInsert(modify, pc);
        }
    }

    private static void checkUpdateDelete(LogicalTableModify modify, PlannerContext pc) {
        List<String> updateColumnList = modify.getUpdateColumnList();
        List<RelOptTable> tables = modify.getTargetTables();
        final Set<RelOptTable> targetTableSet = modify.getTableInfo().getTargetTableSet();

        if (modify.isUpdate()) {
            final boolean enableModifyShardingColumn = getProperty(pc, ConnectionParams.ENABLE_MODIFY_SHARDING_COLUMN);
            if (targetTableSet.size() > 1 || !enableModifyShardingColumn) {
                /*
                  DO NOT allow multi-table update to modify shardColumns.
                  DO NOT allow single-table update to modify shardColumns by default.
                 */
                checkModifyShardingColumn(updateColumnList, tables, (c, t) -> {
                    throw new TddlRuntimeException(ERR_MODIFY_SHARD_COLUMN, c, Util.last(t.getQualifiedName()));
                });
            } else {
                /*
                  DO NOT allow single-table update to modify shardColumns on table without primary key
                 */
                checkModifyShardingColumn(updateColumnList, tables, (c, t) -> {
                    final Pair<String, String> qualifiedTableName = RelUtils.getQualifiedTableName(t);
                    final String schemaName = qualifiedTableName.left;
                    final String tableName = qualifiedTableName.right;

                    if (GeneralUtil.isEmpty(pc.getExecutionContext().getSchemaManager(schemaName)
                        .getTable(tableName)
                        .getPrimaryKey())) {
                        throw new TddlRuntimeException(ERR_MODIFY_SHARD_COLUMN_ON_TABLE_WITHOUT_PK, c,
                            Util.last(t.getQualifiedName()));
                    }
                });
            }
        }

        if (!getProperty(pc, ConnectionParams.DML_ON_GSI)) {
            for (RelOptTable table : targetTableSet) {
                final Pair<String, String> qualifiedTableName = RelUtils.getQualifiedTableName(table);
                final String schemaName = qualifiedTableName.left;
                final String tableName = qualifiedTableName.right;

                if (GlobalIndexMeta.isGsiTable(tableName, schemaName, pc.getExecutionContext())) {
                    throw new TddlRuntimeException(ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY, tableName);
                }
            }
        }
    }

    public static boolean checkModifyShardingColumn(LogicalModify modify) {
        if (!modify.isUpdate()) {
            return false;
        }

        final List<String> updateColumnList = modify.getUpdateColumnList();
        final List<RelOptTable> tables = modify.getTargetTables();

        return checkModifyShardingColumn(updateColumnList, tables, (c, t) -> {
        });
    }

    public static boolean checkModifyShardingColumn(List<String> updateColumnList, List<RelOptTable> tables,
                                                    BiConsumer<String, RelOptTable> handler) {
        for (int i = 0; i < updateColumnList.size(); i++) {
            final RelOptTable tableMeta = tables.get(i);
            final Pair<String, String> qualifiedTableName = RelUtils.getQualifiedTableName(tableMeta);
            final String schemaName = qualifiedTableName.left;
            final String tableName = qualifiedTableName.right;
            final String columnName = updateColumnList.get(i);

            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                if (OptimizerContext.getContext(schemaName)
                    .getRuleManager()
                    .getSharedColumns(tableName)
                    .stream()
                    .anyMatch(s -> s.equalsIgnoreCase(columnName))) {
                    handler.accept(columnName, tableMeta);
                    return true;
                }
            } else {
                if (OptimizerContext.getContext(schemaName)
                    .getRuleManager()
                    .getActualSharedColumns(tableName)
                    .stream()
                    .anyMatch(s -> s.equalsIgnoreCase(columnName))) {
                    handler.accept(columnName, tableMeta);
                    return true;
                }
            }
//            if (OptimizerContext.getContext(schemaName)
//                .getRuleManager()
//                .getSharedColumns(tableName)
//                .stream()
//                .anyMatch(s -> s.equalsIgnoreCase(columnName))) {
//                handler.accept(columnName, tableMeta);
//                return true;
//            }
        }

        return false;
    }

    private static void checkInsert(LogicalTableModify modify, PlannerContext pc) {
        if (!getProperty(pc, ConnectionParams.DML_ON_GSI)) {
            List<String> qualifiedName = modify.getTable().getQualifiedName();
            String tableName = Util.last(qualifiedName);
            String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;

            if (GlobalIndexMeta.isGsiTable(tableName, schemaName, pc.getExecutionContext())) {
                throw new TddlRuntimeException(ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY, tableName);
            }
        }
    }

    /**
     * If it's an INSERT SELECT statement, and its target table has auto
     * increment primary key, or it has no primary key, and it's in transaction,
     * throw an exception. Note that we don't check for PhyTableOperation or,
     * because it has been checked.
     */
    public static void check(DirectTableOperation operation) {
        if (operation.getKind() != SqlKind.INSERT && operation.getKind() != SqlKind.REPLACE) {
            return;
        }

//        String schemaName = operation.getSchemaName();
//        String tableName = operation.getTableNames().get(0);
//
//        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getSchemaManager().getTable(tableName);
//        if (!tableMeta.isHasPrimaryKey()) {
//            String msg = "Insertion into table without primary key in transaction is not supported";
//            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_UNSUPPORTED, msg);
//        }
//
//        List<String> autoIncrementColumns = tableMeta.getAutoIncrementColumns();
//        if (autoIncrementColumns.isEmpty()) {
//            return;
//        }
//
//        if (!SequenceManagerProxy.getInstance().isUsingSequence(schemaName, tableName)) {
//            String columnName = autoIncrementColumns.get(0);
//            String msg = String.format("Insertion into auto increment column(`%s`) without sequence in transaction is"
//                + "not supported. Try to use sequence in CREATE TABLE", columnName);
//            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_UNSUPPORTED, msg);
//        }
    }

    private static boolean getProperty(PlannerContext pc, ConfigParam param) {
        Map<String, Object> extraCmds = pc.getExtraCmds();
        if (GeneralUtil.isEmpty(extraCmds)
            || !extraCmds.containsKey(param.getName())) {
            return Boolean.parseBoolean(param.getDefault());
        }

        return Boolean.parseBoolean(extraCmds.get(param.getName()).toString());
    }

    public static void checkModifyBroadcastWithMultiUpdate(TableModify tableModify) {
        checkModifyBroadcast(tableModify, () -> {
            throw new TddlNestableRuntimeException("multi update not support broadcast");
        });
    }

    public static void checkModifyBroadcastWithMultiDelete(TableModify tableModify) {
        checkModifyBroadcast(tableModify, () -> {
            throw new TddlNestableRuntimeException("multi delete not support broadcast");
        });
    }

    public static boolean checkModifyBroadcast(TableModify tableModify, Runnable handler) {
        return checkModifyBroadcast(tableModify.getTargetTables(), handler);
    }

    public static boolean checkModifyBroadcast(List<RelOptTable> targetTables, Runnable handler) {
        return targetTables.stream().anyMatch(t -> {
            final Pair<String, String> schemaTable = RelUtils.getQualifiedTableName(t);
            final TddlRuleManager or = OptimizerContext.getContext(schemaTable.left).getRuleManager();
            if (or.isBroadCast(schemaTable.right)) {
                handler.run();
                return true;
            }
            return false;
        });
    }

    //for the table which is in scaleout writable phase, we could not push down it directly
    public static boolean isAllTablesCouldPushDown(TableModify tableModify, ExecutionContext ec) {
        return tableModify.getTargetTables().stream().allMatch(t -> {
            final Pair<String, String> schemaTable = RelUtils.getQualifiedTableName(t);
            final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaTable.left);
            assert optimizerContext != null;
            final TableMeta tableMeta = ec.getSchemaManager(schemaTable.left).getTable(schemaTable.right);
            if (ComplexTaskPlanUtils.canDelete(tableMeta)) {
                return false;
            } else {
                return true;
            }
        });
    }

    public static boolean checkModifyGsi(TableModify tableModify, ExecutionContext ec) {
        final List<String> targetColumns = new ArrayList<>();
        if (tableModify.isUpdate()) {
            targetColumns.addAll(tableModify.getUpdateColumnList());
        }
        final List<RelOptTable> targetTables = tableModify.getTargetTables();
        final boolean isDelete = tableModify.isDelete();

        return checkModifyGsi(targetTables, targetColumns, isDelete, ec);
    }

    public static boolean checkModifyGsi(List<RelOptTable> targetTables, List<String> targetColumns, boolean isDelete,
                                         ExecutionContext ec) {
        return Ord.zip(targetTables).stream().anyMatch(o -> {
            final RelOptTable t = o.getValue();
            final List<TableMeta> indexMeta = GlobalIndexMeta.getIndex(t, ec);

            if (indexMeta.isEmpty()) {
                // Without gsi
                return false;
            } else if (isDelete) {
                // DELETE on table with gsi
                return true;
            }

            // Whether UPDATE modify columns belong to gsi
            final String column = targetColumns.get(o.getKey());
            return indexMeta.stream().anyMatch(tm -> null != tm.getColumn(column));
        });
    }

    public static boolean checkModifyGsi(RelOptTable targetTable, Collection<String> targetColumns,
                                         ExecutionContext ec) {
        final List<TableMeta> indexMeta = GlobalIndexMeta.getIndex(targetTable, ec);

        if (indexMeta.isEmpty()) {
            // Without gsi
            return false;
        }

        // Whether UPDATE modify columns belong to gsi
        return indexMeta.stream().anyMatch(tm -> targetColumns.stream().anyMatch(c -> null != tm.getColumn(c)));
    }

    public static boolean checkGsiHasAutoUpdateColumns(List<TableModify.TableInfoNode> srcTables, ExecutionContext ec) {
        return srcTables.stream().anyMatch(t -> {
            RelOptTable targetTable = t.getRefTable();
            final List<TableMeta> indexMeta = GlobalIndexMeta.getIndex(targetTable, ec);
            if (indexMeta.isEmpty()) {
                // Without gsi
                return false;
            }

            final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
            final TableMeta tableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);
            final List<String> autoUpdateColumns =
                tableMeta.getAutoUpdateColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
            return indexMeta.stream().anyMatch(tm -> autoUpdateColumns.stream().anyMatch(c -> null != tm.getColumn(c)));
        });
    }

    public static boolean checkModifyPk(TableModify tableModify, ExecutionContext ec) {
        if (!tableModify.isUpdate()) {
            return false;
        }

        final List<String> targetColumns = tableModify.getUpdateColumnList();
        final List<RelOptTable> targetTables = tableModify.getTargetTables();

        return Ord.zip(targetTables).stream().anyMatch(o -> {
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(o.getValue());
            final TableMeta tm = ec.getSchemaManager(qn.left).getTable(qn.right);
            final Set<String> pkSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

            pkSet.addAll(GlobalIndexMeta.getPrimaryKeys(tm));

            return pkSet.contains(targetColumns.get(o.getKey()));
        });
    }

    public static boolean checkModifyShardingColumnWithGsi(LogicalModify modify, ExecutionContext ec) {
        if (!modify.isUpdate()) {
            return false;
        }

        final List<RelOptTable> tables = modify.getTargetTables();
        final List<String> updateColumnList = modify.getUpdateColumnList();

        return checkModifyShardingColumnWithGsi(tables, updateColumnList, ec);
    }

    public static boolean checkModifyShardingColumnWithGsi(List<RelOptTable> targetTables, List<String> targetColumns,
                                                           ExecutionContext ec) {
        final Map<Integer, List<TableMeta>> tableGsiMap = IntStream.range(0, targetTables.size()).boxed()
            .map(i -> Pair.of(i, GlobalIndexMeta.getIndex(targetTables.get(i), ec)))
            .collect(Collectors.toMap(p -> p.left, p -> p.right, (o, n) -> o));
        final List<Integer> targetTableIndexes =
            IntStream.range(0, targetTables.size()).boxed().collect(Collectors.toList());

        final Map<Integer, List<Integer>> primaryUpdateColumnMappings = new HashMap<>();
        final Map<Integer, List<List<Integer>>> gsiUpdateColumnMappings = new HashMap<>();
        BuildPlanUtils.buildColumnMappings(targetColumns, targetTableIndexes, tableGsiMap, primaryUpdateColumnMappings,
            gsiUpdateColumnMappings);

        for (List<Integer> mapping : primaryUpdateColumnMappings.values()) {
            final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
            final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
            final List<RelOptTable> updateTables = Mappings.permute(targetTables, updateColumnMapping);

            if (checkModifyShardingColumn(updateColumns, updateTables, (x, y) -> {
            })) {
                return true;
            }
        }

        for (List<List<Integer>> mappings : gsiUpdateColumnMappings.values()) {
            if (GeneralUtil.isEmpty(mappings)) {
                continue;
            }
            for (List<Integer> mapping : mappings) {
                final Mapping updateColumnMapping = Mappings.source(mapping, targetColumns.size());
                final List<String> updateColumns = Mappings.permute(targetColumns, updateColumnMapping);
                final List<RelOptTable> updateTables = Mappings.permute(targetTables, updateColumnMapping);

                if (checkModifyShardingColumn(updateColumns, updateTables, (x, y) -> {
                })) {
                    return true;
                }
            }
        }
        return false;
    }
}
