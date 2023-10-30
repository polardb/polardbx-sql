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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.MappingBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdScan;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlReferenceOption;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author wenki
 */
public class ForeignKeyUtils {
    public static final int MAX_FK_DEPTH = 15;
    public static final String PARTITION_FK_SUB_JOB = " /* partition_fk_sub_job */";

    public static void buildForeignKeySubPlans(ExecutionContext ec, ExecutionPlan executionPlan,
                                               PlannerContext plannerContext) {
        final boolean checkForeignKey = ec.foreignKeyChecks();
        if (checkForeignKey && (executionPlan.getPlan() instanceof LogicalModify
            || executionPlan.getPlan() instanceof LogicalRelocate)) {
            TableModify plan = (TableModify) executionPlan.getPlan();

            if (plan instanceof LogicalModify && plan.getTargetTableNames().size() > 1) {
                LogicalModify logicalModify = (LogicalModify) executionPlan.getPlan();
                Set<Integer> tableIndexes = new TreeSet<>(logicalModify.getTargetTableIndexes());
                for (int i = 0; i < tableIndexes.size(); i++) {
                    LogicalModify modify = null;
                    DistinctWriter writer = logicalModify.getPrimaryModifyWriters().get(i);
                    if (writer instanceof SingleModifyWriter) {
                        modify = ((SingleModifyWriter) writer).getModify();
                    } else if (writer instanceof BroadcastModifyWriter) {
                        modify = ((BroadcastModifyWriter) writer).getModify();
                    } else {
                        modify = ((ShardingModifyWriter) writer).getModify();
                    }

                    ForeignKeyUtils.buildFkPlans(plan.getSchemaName(),
                        modify.getLogicalTableName(), plan, modify, null,
                        null, plannerContext, false, 1);
                }
            } else if (plan instanceof LogicalRelocate && plan.getTargetTableNames().size() > 1) {
                LogicalRelocate logicalRelocate = (LogicalRelocate) executionPlan.getPlan();
                final Map<Integer, DistinctWriter> primaryDistinctWriter = logicalRelocate.getPrimaryDistinctWriter();
                final Map<Integer, RelocateWriter> primaryRelocateWriter = logicalRelocate.getPrimaryRelocateWriter();

                for (Integer tableIndex : logicalRelocate.getSetColumnMetas().keySet()) {
                    LogicalModify modify = null;
                    DistinctWriter writer = primaryRelocateWriter.containsKey(tableIndex) ?
                        primaryRelocateWriter.get(tableIndex).getModifyWriter() :
                        primaryDistinctWriter.get(tableIndex);

                    if (writer instanceof SingleModifyWriter) {
                        modify = ((SingleModifyWriter) writer).getModify();
                    } else if (writer instanceof BroadcastModifyWriter) {
                        modify = ((BroadcastModifyWriter) writer).getModify();
                    } else {
                        modify = ((ShardingModifyWriter) writer).getModify();
                    }

                    ForeignKeyUtils.buildFkPlans(plan.getSchemaName(),
                        modify.getLogicalTableName(), plan, modify, null,
                        null, plannerContext, false, 1);
                }
            } else {
                ForeignKeyUtils.buildFkPlans(plan.getSchemaName(),
                    plan.getTargetTableNames().get(0), plan, plan, null,
                    null, plannerContext, false, 1);
            }
        }
    }

    public static void buildFkPlans(String schemaName, String tableName, TableModify modify, TableModify currentModify,
                                    List<String> updateColumnList, Map<String, String> columnMap,
                                    PlannerContext plannerContext, boolean alreadySetNull, int depth) {
        if (depth > MAX_FK_DEPTH) {
            return;
        }

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
            .getTableWithNull(tableName);

        List<Pair<String, ForeignKeyData>> referencedForeignKeysWithIndex = new ArrayList<>();
        if (columnMap == null) {
            referencedForeignKeysWithIndex =
                tableMeta.getReferencedForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());
        } else {
            for (Map.Entry<String, ForeignKeyData> e : tableMeta.getReferencedForeignKeys().entrySet()) {
                if (new HashSet<>(e.getValue().refColumns).containsAll(columnMap.values())) {
                    referencedForeignKeysWithIndex.add(new Pair<>(e.getKey(), e.getValue()));
                }
            }
        }

        if (referencedForeignKeysWithIndex.isEmpty()) {
            return;
        }

        for (Pair<String, ForeignKeyData> fk : referencedForeignKeysWithIndex) {
            ForeignKeyData data = fk.getValue();
            String refSchema = data.schema;
            String refTableName = data.tableName;
            String constraintName = data.constraint;
            List<String> columns = data.columns;

            TableMeta refTableMeta = OptimizerContext.getContext(refSchema).getLatestSchemaManager()
                .getTableWithNull(refTableName);

            boolean deleteCascade = currentModify.isDelete() && !alreadySetNull && data.onDelete != null && (
                data.onDelete == ForeignKeyData.ReferenceOptionType.CASCADE);

            boolean updateCascade = (currentModify.isUpdate() || alreadySetNull) && data.onUpdate != null && (
                data.onUpdate == ForeignKeyData.ReferenceOptionType.CASCADE);

            boolean deleteUpdateSetNull = data.onDelete == ForeignKeyData.ReferenceOptionType.SET_NULL ||
                data.onUpdate == ForeignKeyData.ReferenceOptionType.SET_NULL;

            List<String> tableColumns =
                refTableMeta.getAllColumns().stream().map(c -> c.getName()).collect(Collectors.toList());

            if (updateCascade) {
                if (depth == 1) {
                    if (!containsUpdateFkColumns(currentModify, data)) {
                        return;
                    }
                }

                final SqlNode originPlan =
                    currentModify instanceof LogicalModify ? ((LogicalModify) currentModify).getOriginalSqlNode() :
                        ((LogicalRelocate) currentModify).getOriginalSqlNode();

                columnMap = IntStream.range(0, data.columns.size()).collect(TreeMaps::caseInsensitiveMap,
                    (m, i) -> m.put(data.refColumns.get(i), data.columns.get(i)),
                    Map::putAll);

                updateColumnList = getUpdateColumnList(depth, updateColumnList, currentModify, columnMap, data);

                List<Pair<String, SqlTypeName>> updateColumnPairList =
                    ForeignKeyUtils.getColumnPairList(updateColumnList, refTableMeta);

                MappingBuilder mappingBuilder =
                    MappingBuilder.create(ListUtils.union(tableColumns, updateColumnList), false);

                final Mapping fkMapping = mappingBuilder.source(updateColumnList).buildMapping();

                final AtomicInteger paramIndex = new AtomicInteger(0);
                final List<SqlIdentifier> targetColumns = new ArrayList<>();
                final SqlNodeList expressionList = new SqlNodeList(SqlParserPos.ZERO);

                // Construct target table
                final SqlIdentifier targetTable =
                    new SqlIdentifier(ImmutableList.of(refSchema, refTableName), SqlParserPos.ZERO);

                // columnList / expressionList
                updateColumnPairList.forEach(c -> {
                    final int index = paramIndex.getAndIncrement();
                    targetColumns.add(new SqlIdentifier(ImmutableList.of(c.left), SqlParserPos.ZERO));
                    SqlNode exp = new SqlDynamicParam(index + 2, c.right, SqlParserPos.ZERO);
                    expressionList.add(exp);
                });

                final SqlNode condition = buildAndCondition(updateColumnPairList, paramIndex);

                final SqlUpdate sqlUpdate = new SqlUpdate(SqlParserPos.ZERO,
                    targetTable,
                    new SqlNodeList(targetColumns, SqlParserPos.ZERO),
                    expressionList,
                    condition,
                    null,
                    null,
                    null,
                    null,
                    currentModify.isUpdate() ? ((SqlUpdate) originPlan).getKeywords() :
                        ((SqlDelete) originPlan).getKeywords()
                );

                ExecutionPlan executionPlan = Planner.getInstance().getPlan(sqlUpdate, plannerContext);

                if (executionPlan.getPlan() instanceof LogicalModify) {
                    LogicalModify logicalModify = (LogicalModify) executionPlan.getPlan();
                    for (DistinctWriter distinctWriter : logicalModify.getPrimaryModifyWriters()) {
                        if (distinctWriter instanceof SingleModifyWriter) {
                            ((SingleModifyWriter) distinctWriter).setUpdateSetMapping(fkMapping);
                        } else if (distinctWriter instanceof BroadcastModifyWriter) {
                            ((BroadcastModifyWriter) distinctWriter).setUpdateSetMapping(fkMapping);
                        } else {
                            ((ShardingModifyWriter) distinctWriter).setUpdateSetMapping(fkMapping);
                        }
                    }
                }

                modify.putFkPlan(refSchema, refTableName, constraintName, executionPlan.getPlan(), depth);

            } else if (deleteCascade) {
                final AtomicInteger paramIndex = new AtomicInteger(0);

                // Construct target table
                final SqlIdentifier targetTable =
                    new SqlIdentifier(ImmutableList.of(refSchema, refTableName), SqlParserPos.ZERO);

                List<Pair<String, SqlTypeName>> deleteColumnPairList =
                    ForeignKeyUtils.getColumnPairList(columns, refTableMeta);

                final SqlNode condition = buildAndCondition(deleteColumnPairList, paramIndex);

                SqlNode sqlDelete = new SqlDelete(SqlParserPos.ZERO, targetTable, condition, null, null);

                ExecutionPlan executionPlan = Planner.getInstance().getPlan(sqlDelete, plannerContext);
                modify.putFkPlan(refSchema, refTableName, constraintName, executionPlan.getPlan(), depth);
            } else if (deleteUpdateSetNull) {
                final SqlNode originPlan =
                    currentModify instanceof LogicalModify ? ((LogicalModify) currentModify).getOriginalSqlNode() :
                        ((LogicalRelocate) currentModify).getOriginalSqlNode();

                updateColumnList = data.columns;

                List<Pair<String, SqlTypeName>> updateColumnPairList =
                    ForeignKeyUtils.getColumnPairList(updateColumnList, refTableMeta);

                MappingBuilder mappingBuilder =
                    MappingBuilder.create(ListUtils.union(tableColumns, updateColumnList), false);

                final Mapping fkMapping = mappingBuilder.source(updateColumnList).buildMapping();

                final AtomicInteger paramIndex = new AtomicInteger(0);
                final List<SqlIdentifier> targetColumns = new ArrayList<>();
                final SqlNodeList expressionList = new SqlNodeList(SqlParserPos.ZERO);

                // Construct target table
                final SqlIdentifier targetTable =
                    new SqlIdentifier(ImmutableList.of(refSchema, refTableName), SqlParserPos.ZERO);

                // columnList / expressionList
                updateColumnPairList.forEach(c -> {
                    final int index = paramIndex.getAndIncrement();
                    targetColumns.add(new SqlIdentifier(ImmutableList.of(c.left), SqlParserPos.ZERO));
                    SqlNode exp = new SqlDynamicParam(index + 2, c.right, SqlParserPos.ZERO);
                    expressionList.add(exp);
                });

                final SqlNode condition = buildAndCondition(updateColumnPairList, paramIndex);

                final SqlUpdate sqlUpdate = new SqlUpdate(SqlParserPos.ZERO,
                    targetTable,
                    new SqlNodeList(targetColumns, SqlParserPos.ZERO),
                    expressionList,
                    condition,
                    null,
                    null,
                    null,
                    null,
                    currentModify.isUpdate() ? ((SqlUpdate) originPlan).getKeywords() :
                        ((SqlDelete) originPlan).getKeywords()
                );

                ExecutionPlan executionPlan = Planner.getInstance().getPlan(sqlUpdate, plannerContext);

                if (executionPlan.getPlan() instanceof LogicalModify) {
                    LogicalModify logicalModify = (LogicalModify) executionPlan.getPlan();
                    for (DistinctWriter distinctWriter : logicalModify.getPrimaryModifyWriters()) {
                        if (distinctWriter instanceof SingleModifyWriter) {
                            ((SingleModifyWriter) distinctWriter).setUpdateSetMapping(fkMapping);
                        } else if (distinctWriter instanceof BroadcastModifyWriter) {
                            ((BroadcastModifyWriter) distinctWriter).setUpdateSetMapping(fkMapping);
                        } else {
                            ((ShardingModifyWriter) distinctWriter).setUpdateSetMapping(fkMapping);
                        }
                    }
                }

                modify.putFkPlan(refSchema, refTableName, constraintName, executionPlan.getPlan(), depth);

                alreadySetNull = true;

            } else {
                return;
            }

            buildFkPlans(refSchema, refTableName, modify, currentModify, updateColumnList, columnMap, plannerContext,
                alreadySetNull,
                depth + 1);
        }

    }

    public static List<List<Object>> getUpdateValueList(ForeignKeyData data, List<String> updateColumns,
                                                        List<List<Object>> values, TableMeta tableMeta,
                                                        boolean isFront) {
        List<String> columns = isFront ? data.columns : data.refColumns;

        List<List<Object>> updateValueList = new ArrayList<>();
        List<Integer> refColIndex = new ArrayList<>();
        for (int i = 0; i < tableMeta.getAllColumns().size(); i++) {
            String columnName = tableMeta.getAllColumns().get(i).getName();
            if (columns.stream().anyMatch(colName -> colName.equalsIgnoreCase(columnName))
                && updateColumns.contains(columnName)) {
                refColIndex.add(updateColumns.indexOf(columnName));
            }
        }

        int columnNumber = tableMeta.getAllColumns().size();
        for (List<Object> row : values) {
            List<Object> updateValue = new ArrayList<>();
            for (Integer colIndex : refColIndex) {
                updateValue.add(row.get(colIndex + columnNumber));
            }
            updateValueList.add(updateValue);
        }
        return updateValueList;
    }

    public static List<Pair<String, SqlTypeName>> getColumnPairList(List<String> fkColumns, TableMeta tableMeta) {
        List<Pair<String, SqlTypeName>> updateColumnList = new ArrayList<>();
        for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
            String columnName = columnMeta.getName();
            if (fkColumns.stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                updateColumnList.add(new Pair<>(columnName, columnMeta.getField().getRelType().getSqlTypeName()));
            }
        }
        return updateColumnList;
    }

    public static boolean containsUpdateFkColumns(TableModify modify, ForeignKeyData data) {
        List<String> intersection = new ArrayList<>();
        for (String t : modify.getUpdateColumnList()) {
            if (data.refColumns.stream().anyMatch(t::equalsIgnoreCase)) {
                intersection.add(t);
                break;
            }
        }
        return !intersection.isEmpty();
    }

    public static SqlNode buildAndCondition(List<Pair<String, SqlTypeName>> columns, AtomicInteger paramIndex) {
        List<SqlNode> equalNodes = new ArrayList<>(columns.size());
        for (Pair<String, SqlTypeName> column : columns) {
            SqlIdentifier sqlIdentifier = new SqlIdentifier(column.left, SqlParserPos.ZERO);
            SqlDynamicParam dynamicParam =
                new SqlDynamicParam(paramIndex.getAndIncrement(), column.right, SqlParserPos.ZERO);
            SqlNode equal = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                new SqlNode[] {sqlIdentifier, dynamicParam},
                SqlParserPos.ZERO);
            equalNodes.add(equal);
        }
        return RelUtils.buildAndTree(equalNodes);
    }

    public static List<String> getUpdateColumnList(int depth, List<String> updateColumnList, TableModify tableModify,
                                                   Map<String, String> columnMap,
                                                   ForeignKeyData data) {
        List<String> refUpdateColumnList = new ArrayList<>();

        if (depth == 1) {
            for (String column : tableModify.getUpdateColumnList()) {
                if (data.refColumns.stream().anyMatch(c -> c.equalsIgnoreCase(column))) {
                    refUpdateColumnList.add(columnMap.get(column));
                }
            }
        } else {
            updateColumnList.forEach(c -> refUpdateColumnList.add(columnMap.get(c)));
        }
        return refUpdateColumnList;
    }

    public static Map<String, Set<String>> getAllForeignKeyRelatedTables(String schemaName, String tableName) {
        Map<String, Set<String>> tables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(tableName);
        if (tableMeta == null) {
            return tables;
        }
        LinkedList<ForeignKeyData> fkQueue = new LinkedList<>(tableMeta.getForeignKeys().values());

        while (!fkQueue.isEmpty()) {
            ForeignKeyData data = fkQueue.remove();
            String refSchemaName = data.refSchema;
            String refTableName = data.refTableName;

            Set<String> t =
                tables.computeIfAbsent(refSchemaName, x -> new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
            t.add(refTableName);

            TableMeta refTableMeta =
                OptimizerContext.getContext(refSchemaName).getLatestSchemaManager().getTableWithNull(refTableName);
            if (refTableMeta == null) {
                continue;
            }

            for (ForeignKeyData fk : refTableMeta.getForeignKeys().values()) {
                if (GeneralUtil.isNotEmpty(tables.get(fk.refSchema)) &&
                    tables.get(fk.refSchema).contains(fk.refTableName)) {
                    continue;
                }
                fkQueue.push(fk);
            }
        }
        return tables;
    }

    public static String getForeignKeyConstraintName(String schemaName, String tableName) {
        // create foreign key constraints symbol
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Set<String> symbols = tableMeta.getForeignKeys().values().stream().map(fk -> fk.constraint)
            .collect(Collectors.toCollection(HashSet::new));
        String baseName = tableName.toLowerCase() + "_ibfk_";
        int prob = 1;
        while (symbols.contains(baseName + prob)) {
            ++prob;
        }

        return baseName + prob;
    }

    public static void checkSetForeignKey(SqlAlterTable sqlAlterTable, PlannerContext plannerContext, String tbName) {
        final SqlAddForeignKey foreignKey = (SqlAddForeignKey) sqlAlterTable.getAlters().get(0);

        // Fake one meta and do check push down.
        final ForeignKeyData foreignKeyData = new ForeignKeyData();
        foreignKeyData.constraint = foreignKey.getConstraint() != null ?
            SQLUtils.normalize(foreignKey.getConstraint().getLastName()) : null;
        foreignKeyData.indexName = foreignKey.getIndexName() != null ?
            SQLUtils.normalize(foreignKey.getIndexName().getLastName()) : null;
        foreignKeyData.columns = foreignKey.getIndexDef().getColumns().stream()
            .map(c -> c.getColumnNameStr().toLowerCase()).collect(Collectors.toList());
        foreignKeyData.refSchema = foreignKey.getSchemaName();
        foreignKeyData.refTableName =
            SQLUtils.normalize(
                foreignKey.getReferenceDefinition().getTableName().getLastName().toLowerCase());
        foreignKeyData.refColumns =
            foreignKey.getReferenceDefinition().getColumns().stream()
                .map(c -> c.getLastName().toLowerCase()).collect(Collectors.toList());

        if (foreignKey.getReferenceDefinition().getreferenceOptions() != null) {
            for (SqlReferenceOption option : foreignKey.getReferenceDefinition()
                .getreferenceOptions()) {
                if (option.getOnType() == SqlReferenceOption.OnType.ON_UPDATE) {
                    foreignKeyData.onUpdate =
                        option.convertReferenceOptionType(option.getReferenceOptionType());
                }
                if (option.getOnType() == SqlReferenceOption.OnType.ON_DELETE) {
                    foreignKeyData.onDelete =
                        option.convertReferenceOptionType(option.getReferenceOptionType());
                }
            }
        }

        foreignKey.setForeignKeyData(foreignKeyData);

        if (ExecutionStrategy.pushableForeignConstraint(plannerContext, foreignKey.getSchemaName(),
            tbName,
            new Pair<>(foreignKeyData.refTableName, foreignKeyData))) {
            // Can push down.
            foreignKey.setPushDown(true);
            foreignKeyData.setPushDown(foreignKey.isPushDown());
        }

        // Scan hint bypass.
        final List<HintCmdOperator> cmdHints =
            HintUtil.collectHint(sqlAlterTable.getHints(), new HintConverter.HintCollection(), false,
                plannerContext.getExecutionContext()).cmdHintResult;
        if (cmdHints.stream().anyMatch(hint -> hint instanceof HintCmdScan)) {
            ((SqlAddForeignKey) sqlAlterTable.getAlters().get(0)).setPushDown(true);
        }

        // Remove referenced table replacement.
        if (!foreignKey.isPushDown()) {
            sqlAlterTable.setLogicalReferencedTables(null);
        }
    }
}
