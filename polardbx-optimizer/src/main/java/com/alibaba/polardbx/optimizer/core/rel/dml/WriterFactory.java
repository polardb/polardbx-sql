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

package com.alibaba.polardbx.optimizer.core.rel.dml;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.MappingBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastInsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.DistinctInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.DistinctWriterWrapper;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateBroadcastInsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateBroadcastInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateBroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateDistinctInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateSingleInsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateSingleInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicateSingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicationInsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicationInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplicationShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleInsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleInsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertRelocateGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertWriter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.core.TableModify.TableInfo;
import org.apache.calcite.rel.core.TableModify.TableInfoNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_PK_WRITER_ON_TABLE_WITHOUT_PK;

/**
 * @author chenmo.cm
 */
public class WriterFactory {

    /**
     * Create update writer for UPDATE/DELETE on sharding table with gsi
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param updateColumnList Update columns
     * @param updateSourceMapping Mapping from updateColumnList to output columns of input rel
     * @return ShardingModifyWriter
     */
    public static ShardingModifyWriter createUpdateWriter(TableModify parent, RelOptTable targetTable,
                                                          Integer primaryTableIndex,
                                                          List<String> updateColumnList,
                                                          List<Integer> updateSourceMapping, ExecutionContext ec) {
        final SqlUpdate originUpdate = (SqlUpdate) ((LogicalModify) parent).getOriginalSqlNode();

        return createUpdateWriter(parent, targetTable, primaryTableIndex, updateColumnList, updateSourceMapping,
            getKeywords(originUpdate.getKeywords()), ec);
    }

    /**
     * Create update writer for UPDATE/DELETE on sharding table with gsi
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table of target gsi table
     * @param updateColumnList Update columns
     * @param updateSourceMapping Mapping from updateColumnList to output columns of input rel
     * @return ShardingModifyWriter
     */
    private static ShardingModifyWriter createUpdateWriter(TableModify parent, RelOptTable targetTable,
                                                           Integer primaryTableIndex,
                                                           List<String> updateColumnList,
                                                           List<Integer> updateSourceMapping, SqlNodeList keywords,
                                                           ExecutionContext ec) {
        final RelNode input = RelUtils.getRelInput(parent);

        // In case of logical table appear more than once
        final Map<String, Integer> columnIndexMap = parent.getSourceColumnIndexMap().get(primaryTableIndex);
        // Input field count
        final int sourceFieldsCount = input.getRowType().getFieldCount();

        final MappingBuilder mappingBuilder = MappingBuilder.create(columnIndexMap, sourceFieldsCount, false);

        return createUpdateWriter(parent, targetTable, updateColumnList, updateSourceMapping, keywords, mappingBuilder,
            ec);
    }

    /**
     * Create update writer for UPDATE/DELETE/UPSERT on sharding table with gsi
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param updateColumnList Update target column names
     * @param updateSourceMapping Mapping from updateColumnList to update source value index list in input row
     * @return ShardingModifyWriter
     */
    private static ShardingModifyWriter createUpdateWriter(TableModify parent, RelOptTable targetTable,
                                                           List<String> updateColumnList,
                                                           List<Integer> updateSourceMapping, SqlNodeList keywords,
                                                           MappingBuilder mappingBuilder, ExecutionContext ec) {
        final RelNode input = RelUtils.getRelInput(parent);
        final RexBuilder rexBuilder = parent.getCluster().getRexBuilder();

        /*
         * Build update
         */
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(targetTable);
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;

        final OptimizerContext oc = OptimizerContext.getContext(targetSchema);
        final TableMeta targetMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);

        final List<String> skNames = oc.getRuleManager().getSharedColumns(targetTableName);
        final List<ColumnMeta> updateSkMetas = skNames.stream().map(targetMeta::getColumn).collect(Collectors.toList());

        final List<String> conditionColumns = new ArrayList<>();
        final String indexName = getUniqueConditionColumns(targetMeta, conditionColumns, ec, () -> {
            throw new TddlRuntimeException(ERR_PK_WRITER_ON_TABLE_WITHOUT_PK, "Update", targetMeta.getTableName());
        });

        final AtomicInteger paramIndex = new AtomicInteger(0);
        final List<SqlIdentifier> targetColumns = new ArrayList<>();
        final List<SqlDynamicParam> sourceExpressions = new ArrayList<>();
        final List<RexNode> sourceExpressionList = new ArrayList<>();

        updateColumnList.forEach(c -> {
            final int index = paramIndex.getAndIncrement();
            targetColumns.add(new SqlIdentifier(ImmutableList.of(c), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlDynamicParam(index + 2, SqlParserPos.ZERO));
            sourceExpressionList.add(rexBuilder.makeDynamicParam(
                getUpdateColumnType(parent, updateColumnList.get(index), updateSourceMapping.get(index)), index));
        });

        /**
         * <pre>
         * UPDATE ? AS xx FORCE INDEX(PRIMARY) SET xx.a = ?, ...
         * WHERE pk0 = ? AND pk1 = ? AND ...
         * </pre>
         */
        final SqlUpdate sqlUpdate = RelUtils.buildUpdateWithAndCondition(targetTableName,
            targetColumns,
            sourceExpressions,
            conditionColumns,
            keywords,
            indexName,
            new AtomicInteger(paramIndex.get() + 2), ec);

        final TableInfo updateTableInfo = buildUpdateTableInfo(sqlUpdate, targetTable, updateColumnList);

        final LogicalModify update = new LogicalModify(parent.getCluster(),
            parent.getTraitSet(),
            targetTable,
            parent.getCatalogReader(),
            input,
            Operation.UPDATE,
            updateColumnList,
            sourceExpressionList,
            false,
            parent.getKeywords(),
            parent.getHints(),
            parent.getHintContext(),
            updateTableInfo);

        update.setOriginalSqlNode(sqlUpdate);

        // Expressions in SET are at the end of row
        int updateSourceSize = mappingBuilder.getTargetSize();
        int updateColumnSize = Optional.ofNullable(parent.getUpdateColumnList()).map(List::size).orElse(0);
        if (parent instanceof LogicalInsert) {
            // Update for DUPLICATE KEY UPDATE
            updateColumnSize = Optional.ofNullable(parent.getDuplicateKeyUpdateList()).map(List::size).orElse(0);
            updateSourceSize += updateColumnSize;
        }
        final int offset = updateSourceSize - updateColumnSize;

        return createShardingModifyWriter(update, conditionColumns, skNames, updateSkMetas, mappingBuilder,
            updateSourceMapping, offset, updateSourceSize, ec);
    }

    private static RelDataType getUpdateColumnType(TableModify parent, String targetColumnName, Integer sourceIndex) {
        if (parent.isInsert()) {
            // Update source is ON DUPLICATE KEY UPDATE part of INSERT
            final LogicalInsert insert = (LogicalInsert) parent;
            return insert.getTable().getRowType().getField(targetColumnName, true, false).getType();
        } else {
            // Update source is selected row
            final RelNode input = RelUtils.getRelInput(parent);
            final List<RelDataTypeField> inputFields = input.getRowType().getFieldList();

            return inputFields.get(sourceIndex).getType();
        }
    }

    /**
     * Create delete writer for sharding table with gsi
     * Using pk or uk as condition
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table of target gsi table
     * @return ShardingModifyWriter
     */
    public static ShardingModifyWriter createDeleteWriter(TableModify parent, RelOptTable targetTable,
                                                          Integer primaryTableIndex, ExecutionContext ec) {
        final RelNode input = RelUtils.getRelInput(parent);

        /*
         * Build delete
         */
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(targetTable);
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;

        final OptimizerContext oc = OptimizerContext.getContext(targetSchema);
        assert oc != null;
        final TableMeta targetMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);

        final List<String> skNames = oc.getRuleManager().getSharedColumns(targetTableName);
        final List<ColumnMeta> deleteSkMetas = skNames.stream().map(targetMeta::getColumn).collect(Collectors.toList());

        final List<String> conditionColumns = new ArrayList<>();
        final String indexName = getUniqueConditionColumns(targetMeta, conditionColumns, ec, () -> {
            throw new TddlRuntimeException(ERR_PK_WRITER_ON_TABLE_WITHOUT_PK, "Delete", targetMeta.getTableName());
        });

        /**
         * <pre>
         * DELETE yy FROM xx AS yy FORCE INDEX(PRIMARY)
         * WHERE (pk0, pk1, ...) IN ((, ...), ...)
         * </pre>
         */
        final SqlDelete sqlDelete =
            RelUtils.buildDeleteWithInCondition(targetTableName, conditionColumns, indexName, new AtomicInteger(2), ec);

        final TableInfo deleteTableInfo = buildDeleteTableInfo(sqlDelete, targetTable);

        /*
         * Should create LogicalModifyView here, but it is impossible to build a
         * LogicalView with correct Filter at this time because we do not know how many
         * rows will be deleted. So just create LogicalModify with parent as input
         * instead
         */
        final LogicalModify delete = new LogicalModify(parent.getCluster(),
            parent.getTraitSet(),
            targetTable,
            parent.getCatalogReader(),
            input,
            Operation.DELETE,
            null,
            null,
            false,
            null,
            null,
            new OptimizerHint(),
            deleteTableInfo);

        delete.setOriginalSqlNode(sqlDelete);

        // Column index map for target table
        final Map<String, Integer> beforeColumnIndexMap = parent.getSourceColumnIndexMap().get(primaryTableIndex);
        final MappingBuilder mappingBuilder =
            MappingBuilder.create(beforeColumnIndexMap, input.getRowType().getFieldCount(), false);

        return createShardingModifyWriter(delete, conditionColumns, skNames, deleteSkMetas, mappingBuilder,
            ImmutableList.of(), 0, mappingBuilder.getTargetSize(), ec);
    }

    /**
     * Create update writer for broadcast or single table
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table index
     * @param updateColumnList Update columns
     * @param updateSourceMapping Mapping from updateColumnList to output columns of input rel
     * @return ShardingModifyWriter
     */
    public static DistinctWriter createBroadcastOrSingleUpdateWriter(TableModify parent,
                                                                     RelOptTable targetTable,
                                                                     Integer primaryTableIndex,
                                                                     List<String> updateColumnList,
                                                                     List<Integer> updateSourceMapping,
                                                                     boolean isBroadcast,
                                                                     boolean isSingle,
                                                                     ExecutionContext ec
    ) {
        final SqlUpdate originUpdate = (SqlUpdate) ((LogicalModify) parent).getOriginalSqlNode();
        final RelNode input = RelUtils.getRelInput(parent);
        final List<Map<String, Integer>> sourceColumnIndexMap = parent.getSourceColumnIndexMap();

        // In case of logical table appear more than once
        final Map<String, Integer> columnIndexMap = sourceColumnIndexMap.get(primaryTableIndex);
        // Input field count
        final int sourceFieldsCount = input.getRowType().getFieldCount();

        final MappingBuilder mappingBuilder = MappingBuilder.create(columnIndexMap, sourceFieldsCount, false);

        return createBroadcastOrSingleUpdateWriter(parent, targetTable, updateColumnList,
            updateSourceMapping, getKeywords(originUpdate.getKeywords()), mappingBuilder, isBroadcast, isSingle, ec);
    }

    /**
     * Create update writer for broadcast or single table
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param updateColumnList Update columns
     * @param updateSourceMapping Mapping from updateColumnList to output columns of input rel
     * @return ShardingModifyWriter
     */
    private static DistinctWriter createBroadcastOrSingleUpdateWriter(TableModify parent,
                                                                      RelOptTable targetTable,
                                                                      List<String> updateColumnList,
                                                                      List<Integer> updateSourceMapping,
                                                                      SqlNodeList keywords,
                                                                      MappingBuilder mappingBuilder,
                                                                      boolean isBroadcast,
                                                                      boolean isSingle,
                                                                      ExecutionContext ec) {
        final RelNode input = RelUtils.getRelInput(parent);
        final RexBuilder rexBuilder = parent.getCluster().getRexBuilder();

        /*
         * Build update
         */
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(targetTable);
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;

        final OptimizerContext oc = OptimizerContext.getContext(targetSchema);
        final TableMeta targetMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);
        final TableRule tableRule = oc.getRuleManager().getTableRule(targetTableName);

        if (isBroadcast) {
            Preconditions.checkState(oc.getRuleManager().isBroadCast(targetTableName));
        } else if (isSingle) {
            Preconditions.checkState(oc.getRuleManager().isTableInSingleDb(targetTableName));
        }
        if (!targetMeta.isHasPrimaryKey() && !targetMeta.hasGsiImplicitPrimaryKey()) {
            throw new TddlRuntimeException(ERR_PK_WRITER_ON_TABLE_WITHOUT_PK, "Update", targetTableName);
        }

        final List<String> pkNames = ImmutableList
            .copyOf((targetMeta.isHasPrimaryKey() ? targetMeta.getPrimaryKey() : targetMeta.getGsiImplicitPrimaryKey())
                .stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        final AtomicInteger paramIndex = new AtomicInteger(0);
        final List<SqlIdentifier> targetColumns = new ArrayList<>();
        final List<SqlDynamicParam> sourceExpressions = new ArrayList<>();
        final List<RexNode> sourceExpressionList = new ArrayList<>();

        updateColumnList.forEach(c -> {
            final int index = paramIndex.getAndIncrement();
            targetColumns.add(new SqlIdentifier(ImmutableList.of(c), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlDynamicParam(index + 2, SqlParserPos.ZERO));
            sourceExpressionList.add(rexBuilder.makeDynamicParam(
                getUpdateColumnType(parent, updateColumnList.get(index), updateSourceMapping.get(index)), index));
        });

        /**
         * <pre>
         * UPDATE ? AS xx FORCE INDEX(PRIMARY) SET xx.a = ?, ...
         * WHERE pk0 = ? AND pk1 = ? AND ...
         * </pre>
         */
        final SqlUpdate sqlUpdate = RelUtils.buildUpdateWithAndCondition(targetTableName,
            targetColumns,
            sourceExpressions,
            pkNames,
            keywords,
            "PRIMARY",
            new AtomicInteger(paramIndex.get() + 2), ec);

        final TableInfo updateTableInfo = buildUpdateTableInfo(sqlUpdate, targetTable, updateColumnList);

        final LogicalModify update = new LogicalModify(parent.getCluster(),
            parent.getTraitSet(),
            targetTable,
            parent.getCatalogReader(),
            input,
            Operation.UPDATE,
            updateColumnList,
            sourceExpressionList,
            false,
            parent.getKeywords(),
            parent.getHints(),
            parent.getHintContext(),
            updateTableInfo);

        update.setOriginalSqlNode(sqlUpdate);

        // Expressions in SET are at the end of row
        int updateSourceSize = mappingBuilder.getTargetSize();
        int updateColumnSize = Optional.ofNullable(parent.getUpdateColumnList()).map(List::size).orElse(0);
        if (parent instanceof LogicalInsert) {
            // Update for DUPLICATE KEY UPDATE
            updateColumnSize = Optional.ofNullable(parent.getDuplicateKeyUpdateList()).map(List::size).orElse(0);
            updateSourceSize += updateColumnSize;
        }
        final int offset = updateSourceSize - updateColumnSize;

        if (isBroadcast) {
            return createBroadcastModifyWriter(update, pkNames, mappingBuilder, updateSourceMapping, offset,
                updateSourceSize, ec);
        } else if (isSingle) {
            return createSingleModifyWriter(update, pkNames, mappingBuilder, updateSourceMapping, offset,
                updateSourceSize, ec);
        }
        throw new TddlNestableRuntimeException("unknown table type");
    }

    /**
     * Create delete writer for broadcast table without gsi
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table of target gsi table
     * @return ShardingModifyWriter
     */
    public static DistinctWriter createBroadcastDeleteWriter(TableModify parent, RelOptTable targetTable,
                                                             Integer primaryTableIndex, ExecutionContext ec) {
        return createBroadcastOrSingleDeleteWriter(parent, targetTable, primaryTableIndex, true, false, ec);
    }

    /**
     * Create delete writer for broadcast table without gsi
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table of target gsi table
     * @return ShardingModifyWriter
     */
    public static DistinctWriter createSingleDeleteWriter(TableModify parent, RelOptTable targetTable,
                                                          Integer primaryTableIndex,
                                                          ExecutionContext ec) {
        return createBroadcastOrSingleDeleteWriter(parent, targetTable, primaryTableIndex, false, true, ec);
    }

    /**
     * Create delete writer for broadcast/single table with gsi
     *
     * @param parent Origin update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table of target gsi table
     * @return ShardingModifyWriter
     */
    private static DistinctWriter createBroadcastOrSingleDeleteWriter(TableModify parent,
                                                                      RelOptTable targetTable,
                                                                      Integer primaryTableIndex,
                                                                      boolean isBroadcast,
                                                                      boolean isSingle, ExecutionContext ec) {

        final RelNode input = RelUtils.getRelInput(parent);

        /*
         * Build delete
         */
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(targetTable);
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;

        final OptimizerContext oc = OptimizerContext.getContext(targetSchema);
        final TableMeta targetMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);
        final TableRule tableRule = oc.getRuleManager().getTableRule(targetTableName);

        if (isBroadcast) {
            Preconditions.checkState(oc.getRuleManager().isBroadCast(targetTableName));
        } else if (isSingle) {
            Preconditions.checkState(oc.getRuleManager().isTableInSingleDb(targetTableName));
        }
        if (!targetMeta.isHasPrimaryKey() && !targetMeta.hasGsiImplicitPrimaryKey()) {
            throw new TddlRuntimeException(ERR_PK_WRITER_ON_TABLE_WITHOUT_PK, "Delete", targetTableName);
        }

        final List<String> pkNames = ImmutableList
            .copyOf((targetMeta.isHasPrimaryKey() ? targetMeta.getPrimaryKey() : targetMeta.getGsiImplicitPrimaryKey())
                .stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        /**
         * <pre>
         * DELETE yy FROM xx AS yy FORCE INDEX(PRIMARY)
         * WHERE (pk0, pk1, ...) IN ((, ...), ...)
         * </pre>
         */
        final SqlDelete sqlDelete =
            RelUtils.buildDeleteWithInCondition(targetTableName, pkNames, "PRIMARY", new AtomicInteger(2), ec);

        final TableInfo deleteTableInfo = buildDeleteTableInfo(sqlDelete, targetTable);

        /*
         * Should create LogicalModifyView here, but it is impossible to build a
         * LogicalView with correct Filter at this time because we do not know how many
         * rows will be deleted. So just create LogicalModify with parent as input
         * instead
         */
        final LogicalModify delete = new LogicalModify(parent.getCluster(),
            parent.getTraitSet(),
            targetTable,
            parent.getCatalogReader(),
            input,
            Operation.DELETE,
            null,
            null,
            false,
            null,
            null,
            new OptimizerHint(),
            deleteTableInfo);

        delete.setOriginalSqlNode(sqlDelete);

        // Column index map for target table
        final Map<String, Integer> beforeColumnIndexMap = parent.getSourceColumnIndexMap().get(primaryTableIndex);
        final MappingBuilder mappingBuilder =
            MappingBuilder.create(beforeColumnIndexMap, input.getRowType().getFieldCount(), false);

        if (isBroadcast) {
            return createBroadcastModifyWriter(delete, pkNames, mappingBuilder, ImmutableList.of(), 0,
                mappingBuilder.getTargetSize(), ec);
        } else if (isSingle) {
            return createSingleModifyWriter(delete, pkNames, mappingBuilder, ImmutableList.of(), 0,
                mappingBuilder.getTargetSize(), ec);
        }
        throw new TddlNestableRuntimeException("unknown table type");
    }

    public static RelocateWriter createRelocateWriter(TableModify parent, RelOptTable targetTable,
                                                      Integer primaryTableIndex, List<String> updateColumnList,
                                                      List<Integer> updateSourceMapping, TableMeta gsiMeta,
                                                      boolean isGsi,
                                                      PlannerContext plannerContext,
                                                      ExecutionContext ec) {
        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
        final OptimizerContext oc = OptimizerContext.getContext(qn.left);
        assert oc != null;
        final boolean isBroadcast = oc.getRuleManager().isBroadCast(qn.right);
        final boolean isSingle = oc.getRuleManager().isTableInSingleDb(qn.right);

        DistinctWriter deleteWriter;
        if (isBroadcast || isSingle) {
            deleteWriter =
                createBroadcastOrSingleDeleteWriter(parent, targetTable, primaryTableIndex, isBroadcast, isSingle, ec);
        } else {
            deleteWriter = createDeleteWriter(parent, targetTable, primaryTableIndex, ec);
        }

        final DistinctWriter insertWriter = createDistinctInsertWriter(parent,
            targetTable,
            primaryTableIndex,
            updateColumnList,
            updateSourceMapping,
            isBroadcast, ec);

        final SqlUpdate originUpdate = (SqlUpdate) ((LogicalModify) parent).getOriginalSqlNode();

        // For rows whose sharding column will not be modified
        DistinctWriter updateWriter = null;
        if (isBroadcast || isSingle) {
            updateWriter = createBroadcastOrSingleUpdateWriter(parent,
                targetTable,
                primaryTableIndex,
                updateColumnList,
                updateSourceMapping, isBroadcast, isSingle, ec);
        } else {
            updateWriter = createUpdateWriter(parent,
                targetTable,
                primaryTableIndex,
                updateColumnList,
                updateSourceMapping,
                getKeywords(originUpdate.getKeywords()), ec);
        }

        return createRelocateWriter(parent,
            targetTable,
            primaryTableIndex,
            updateColumnList,
            updateSourceMapping,
            deleteWriter,
            insertWriter,
            updateWriter,
            gsiMeta,
            isGsi,
            plannerContext,
            ec);
    }

    public static DistinctWriter createUpdateGsiWriter(TableModify parent, RelOptTable targetTable,
                                                       Integer primaryTableIndex, List<String> updateColumnList,
                                                       List<Integer> updateSourceMapping, TableMeta gsiMeta,
                                                       ExecutionContext ec) {

        final SqlUpdate originUpdate = (SqlUpdate) ((LogicalModify) parent).getOriginalSqlNode();

        boolean isBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
        boolean isSingle = TableTopologyUtil.isSingle(gsiMeta);

        if (isBroadcast) {
            final BroadcastModifyWriter updateWriter =
                (BroadcastModifyWriter) createBroadcastOrSingleUpdateWriter(parent,
                    targetTable,
                    primaryTableIndex,
                    updateColumnList,
                    updateSourceMapping,
                    isBroadcast,
                    isSingle, ec
                );
            final BroadcastModifyWriter deleteWriter =
                (BroadcastModifyWriter) createBroadcastDeleteWriter(parent, targetTable, primaryTableIndex, ec);
            return createBroadcastModifyGsiWriter(updateWriter.getModify(), updateWriter, deleteWriter, gsiMeta);
        } else if (isSingle) {
            final SingleModifyWriter updateWriter = (SingleModifyWriter) createBroadcastOrSingleUpdateWriter(parent,
                targetTable,
                primaryTableIndex,
                updateColumnList,
                updateSourceMapping,
                isBroadcast,
                isSingle, ec
            );
            final SingleModifyWriter deleteWriter =
                (SingleModifyWriter) createSingleDeleteWriter(parent, targetTable, primaryTableIndex, ec);
            return createSingleModifyGsiWriter(updateWriter.getModify(), updateWriter, deleteWriter, gsiMeta);
        } else {
            final ShardingModifyWriter updateWriter = createUpdateWriter(parent,
                targetTable,
                primaryTableIndex,
                updateColumnList,
                updateSourceMapping,
                getKeywords(originUpdate.getKeywords()),
                ec);
            final ShardingModifyWriter deleteWriter = createDeleteWriter(parent, targetTable, primaryTableIndex, ec);
            return createShardingModifyGsiWriter(updateWriter.getModify(), updateWriter, deleteWriter, gsiMeta);
        }
    }

    public static DistinctWriter createDeleteGsiWriter(TableModify parent, RelOptTable targetTable,
                                                       Integer primaryTableIndex, TableMeta gsiMeta,
                                                       ExecutionContext ec) {
        boolean isBroadcast = TableTopologyUtil.isBroadcast(gsiMeta);
        boolean isSingle = TableTopologyUtil.isSingle(gsiMeta);

        if (isBroadcast) {
            final BroadcastModifyWriter deleteWriter =
                (BroadcastModifyWriter) createBroadcastDeleteWriter(parent, targetTable, primaryTableIndex, ec);
            return createBroadcastModifyGsiWriter(deleteWriter.getModify(), null, deleteWriter, gsiMeta);
        } else if (isSingle) {
            final SingleModifyWriter deleteWriter =
                (SingleModifyWriter) createSingleDeleteWriter(parent, targetTable, primaryTableIndex, ec);
            return createSingleModifyGsiWriter(deleteWriter.getModify(), null, deleteWriter, gsiMeta);
        } else {
            final ShardingModifyWriter deleteWriter = createDeleteWriter(parent, targetTable, primaryTableIndex, ec);
            return createShardingModifyGsiWriter(deleteWriter.getModify(), null, deleteWriter, gsiMeta);
        }
    }

    private static RelocateWriter createRelocateWriter(TableModify parent, RelOptTable targetTable,
                                                       Integer primaryTableIndex, List<String> updateColumnList,
                                                       List<Integer> updateSourceMapping,
                                                       DistinctWriter deleteWriter,
                                                       DistinctWriter insertWriter,
                                                       DistinctWriter updateWriter,
                                                       TableMeta gsiMeta,
                                                       boolean isGsi,
                                                       PlannerContext plannerContext,
                                                       ExecutionContext ec) {
        // Build column index map for target table
        final List<Map<String, Integer>> sourceColumnIndexMap = parent.getSourceColumnIndexMap();
        final Map<String, Integer> columnIndexMap = sourceColumnIndexMap.get(primaryTableIndex);

        // Expressions in SET are at the end of row
        final RelDataType srcRowType = parent.getInput().getRowType();
        final int fieldCount = srcRowType.getFieldCount();
        final int offset = fieldCount - parent.getUpdateColumnList().size();
        final List<Integer> setSrc = updateSourceMapping.stream().map(i -> offset + i).collect(Collectors.toList());

        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
        final OptimizerContext oc = OptimizerContext.getContext(qn.left);
        final TableMeta targetTableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);

        final Set<String> identifierKeyNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        identifierKeyNames.addAll(oc.getRuleManager().getSharedColumns(qn.right));

        final boolean allGsiPublished =
            !isGsi || GlobalIndexMeta.isPublished(plannerContext.getExecutionContext(), gsiMeta);

        final boolean isGsiTableCanScaleOutWrite = (gsiMeta != null && ComplexTaskPlanUtils.canWrite(gsiMeta));

        if (!allGsiPublished || ComplexTaskPlanUtils.canWrite(targetTableMeta) || isGsiTableCanScaleOutWrite) {
            final List<String> pkNames = ImmutableList
                .copyOf(targetTableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList()));
            identifierKeyNames.addAll(pkNames);
        }

        final AtomicInteger extraIndex = new AtomicInteger(updateColumnList.size());
        if (parent instanceof LogicalModify
            && GeneralUtil.isNotEmpty(((LogicalModify) parent).getExtraTargetColumns())) {
            extraIndex.addAndGet(-((LogicalModify) parent).getExtraTargetColumns().size());
        }

        // Take set more than once into consideration
        final Map<String, Integer> identifierKeyTargetMap = new LinkedHashMap<>();
        final Map<String, Integer> identifierKeySourceMap = new LinkedHashMap<>();
        final AtomicBoolean modifySkOnly = new AtomicBoolean(true);
        Ord.zip(updateColumnList).forEach(o -> {
            if (identifierKeyNames.contains(o.e)) {
                identifierKeySourceMap.put(o.e, setSrc.get(o.i));
                identifierKeyTargetMap.put(o.e, columnIndexMap.get(o.e));
            } else if (o.i < extraIndex.get()) {
                modifySkOnly.set(false);
            }
        });

        final Mapping identifierKeyTargetMapping =
            Mappings.source(ImmutableList.copyOf(identifierKeySourceMap.values()), fieldCount);
        final Mapping identifierKeySourceMapping =
            Mappings.source(ImmutableList.copyOf(identifierKeyTargetMap.values()), fieldCount);
        final List<ColumnMeta> identifierKeyMetas =
            identifierKeyTargetMap.keySet().stream().map(targetTableMeta::getColumn).collect(
                Collectors.toList());

        if (!isGsi) {
            return new RelocateWriter(targetTable,
                deleteWriter,
                insertWriter,
                updateWriter,
                identifierKeyTargetMapping,
                identifierKeySourceMapping,
                identifierKeyMetas,
                modifySkOnly.get());
        } else {
            return new RelocateGsiWriter(targetTable,
                deleteWriter,
                insertWriter,
                updateWriter,
                identifierKeyTargetMapping,
                identifierKeySourceMapping,
                identifierKeyMetas,
                modifySkOnly.get(),
                gsiMeta);
        }
    }

    private static BroadcastModifyWriter createBroadcastModifyWriter(LogicalModify modify, List<String> pkNames,
                                                                     MappingBuilder mappingBuilder,
                                                                     List<Integer> updateSourceMapping,
                                                                     int updateSourceOffset, int updateSourceSize,
                                                                     ExecutionContext ec) {
        // Build column mapping
        final Mapping pkMapping = mappingBuilder.source(pkNames).buildMapping();
        final Mapping deduplicateMapping = pkMapping;

        Mapping updateSetMapping = null;
        if (modify.isUpdate()) {
            final List<Integer> withOffset =
                updateSourceMapping.stream().map(i -> updateSourceOffset + i).collect(Collectors.toList());
            updateSetMapping = Mappings.source(withOffset, updateSourceSize);
        }
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(modify.getTable());
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;
        TableMeta tableMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);
        boolean isNeedGenReplicaPlan = ComplexTaskPlanUtils.canWrite(tableMeta);
        return isNeedGenReplicaPlan ?
            new ReplicateBroadcastModifyWriter(modify.getTable(), modify, pkMapping, updateSetMapping,
                deduplicateMapping,
                tableMeta) :
            new BroadcastModifyWriter(modify.getTable(), modify, pkMapping, updateSetMapping, deduplicateMapping);
    }

    private static SingleModifyWriter createSingleModifyWriter(LogicalModify modify, List<String> pkNames,
                                                               MappingBuilder mappingBuilder,
                                                               List<Integer> updateSourceMapping,
                                                               int updateSourceOffset, int updateSourceSize,
                                                               ExecutionContext ec) {
        // Build column mapping
        final Mapping pkMapping = mappingBuilder.source(pkNames).buildMapping();
        final Mapping deduplicateMapping = pkMapping;

        Mapping updateSetMapping = null;
        if (modify.isUpdate()) {
            final List<Integer> withOffset =
                updateSourceMapping.stream().map(i -> updateSourceOffset + i).collect(Collectors.toList());
            updateSetMapping = Mappings.source(withOffset, updateSourceSize);
        }
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(modify.getTable());
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;
        TableMeta tableMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);

        boolean isNeedGenReplicaPlan = ComplexTaskPlanUtils.canWrite(tableMeta);

        return isNeedGenReplicaPlan ?
            new ReplicateSingleModifyWriter(modify.getTable(), modify, pkMapping, updateSetMapping, deduplicateMapping,
                tableMeta) :
            new SingleModifyWriter(modify.getTable(), modify, pkMapping, updateSetMapping, deduplicateMapping);
    }

    private static ShardingModifyWriter createShardingModifyWriter(LogicalModify modify, List<String> pkNames,
                                                                   List<String> skNames, List<ColumnMeta> skMetas,
                                                                   MappingBuilder mappingBuilder,
                                                                   List<Integer> updateSourceMapping,
                                                                   int updateSourceOffset,
                                                                   int updateSourceSize,
                                                                   ExecutionContext ec) { // Build column mapping
        final Mapping pkMapping = mappingBuilder.source(pkNames).buildMapping();
        final Mapping skMapping = mappingBuilder.source(skNames).buildMapping();
        final List<String> deduplicateColumns =
            Stream.concat(pkNames.stream(), skNames.stream()).map(String::toLowerCase).sorted().distinct()
                .collect(Collectors.toList());
        final Mapping deduplicateMapping = mappingBuilder.source(deduplicateColumns).buildMapping();

        Mapping updateSetMapping = null;
        if (modify.isUpdate()) {
            final List<Integer> withOffset =
                updateSourceMapping.stream().map(i -> updateSourceOffset + i).collect(Collectors.toList());
            updateSetMapping = Mappings.source(withOffset, updateSourceSize);
        }
        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(modify.getTable());
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;

        final OptimizerContext oc = OptimizerContext.getContext(targetSchema);

        /**
         * When dml is cross schema dml such as 
         * "insert into drds_polarx2_ddl_qatest_app.gxw_test_4 values(1, 'simiao_test');"
         * the targetSchema is different of the schema of ExecutionContext
         */
        final TableMeta targetMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);
        //final TableMeta targetMeta = oc.getLatestSchemaManager().getTable(targetTableName);

        boolean isNeedGenReplicaPlan = ComplexTaskPlanUtils.canDelete(targetMeta);
        return isNeedGenReplicaPlan ? new ReplicationShardingModifyWriter(modify
            .getTable(), modify, skMetas, pkMapping, skMapping, updateSetMapping, deduplicateMapping, targetMeta) :
            new ShardingModifyWriter(modify
                .getTable(), modify, skMetas, pkMapping, skMapping, updateSetMapping, deduplicateMapping);

    }

    private static DistinctWriter createBroadcastModifyGsiWriter(TableModify modify, BroadcastModifyWriter updateWriter,
                                                                 BroadcastModifyWriter deleteWriter,
                                                                 TableMeta gsiMeta) {
        return new BroadcastModifyGsiWriter(modify
            .getTable(), modify.getOperation(), updateWriter, deleteWriter, gsiMeta);
    }

    private static DistinctWriter createSingleModifyGsiWriter(TableModify modify, SingleModifyWriter updateWriter,
                                                              SingleModifyWriter deleteWriter, TableMeta gsiMeta) {
        return new SingleModifyGsiWriter(modify
            .getTable(), modify.getOperation(), updateWriter, deleteWriter, gsiMeta);
    }

    private static ShardingModifyGsiWriter createShardingModifyGsiWriter(TableModify modify,
                                                                         ShardingModifyWriter updateWriter,
                                                                         ShardingModifyWriter deleteWriter,
                                                                         TableMeta gsiMeta) {
        return new ShardingModifyGsiWriter(modify
            .getTable(), modify.getOperation(), updateWriter, deleteWriter, gsiMeta);
    }

    /**
     * DistinctWriter for LogicalRelocate
     *
     * @param parent Original update operator
     * @param targetTable Target table
     * @param primaryTableIndex Primary table of target, if target table is GSI. Otherwise use @param{targetTable}
     * @param updateColumnList Columns to be updated
     * @param updateColumnMapping Mapping from columns to be updated to input row
     */
    public static DistinctWriter createDistinctInsertWriter(TableModify parent, RelOptTable targetTable,
                                                            Integer primaryTableIndex,
                                                            List<String> updateColumnList,
                                                            List<Integer> updateColumnMapping,
                                                            boolean isBroadCast, ExecutionContext ec) {
        final RelNode input = parent.getInput();
        final List<Map<String, Integer>> sourceColumnIndexMap = parent.getSourceColumnIndexMap();

        final Pair<String, String> targetSchemaTable = RelUtils.getQualifiedTableName(targetTable);
        final String targetSchema = targetSchemaTable.left;
        final String targetTableName = targetSchemaTable.right;
        final OptimizerContext oc = OptimizerContext.getContext(targetSchema);
        final TableMeta targetMeta = ec.getSchemaManager(targetSchema).getTable(targetTableName);
        final List<String> skNames = oc.getRuleManager().getSharedColumns(targetTableName);
        final TableRule tableRule = oc.getRuleManager().getTableRule(targetTableName);

        if (!targetMeta.isHasPrimaryKey() && !targetMeta.hasGsiImplicitPrimaryKey()) {
            throw new TddlRuntimeException(ERR_PK_WRITER_ON_TABLE_WITHOUT_PK, "INSERT", targetTableName);
        }

        final List<String> pkNames = ImmutableList
            .copyOf((targetMeta.isHasPrimaryKey() ? targetMeta.getPrimaryKey() : targetMeta.getGsiImplicitPrimaryKey())
                .stream().map(ColumnMeta::getName).collect(Collectors.toList()));

        // Build column index map for target table
        // In case of logical table appear more than once
        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columnIndexMap.putAll(sourceColumnIndexMap.get(primaryTableIndex));
        final int fieldCount = input.getRowType().getFieldCount();

        final TreeSet<String> deduplicateColumns = Stream.concat(pkNames.stream(), skNames.stream())
            .collect(Collectors.toCollection(TreeSet::new));
        final Mapping deduplicateMapping = Mappings
            .source(deduplicateColumns.stream().map(columnIndexMap::get).collect(Collectors.toList()), fieldCount);

        /*
         * Build insert
         */
        final RelDataType targetRowType = RelOptTableImpl.realRowType(targetTable);
        final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final List<RelDataTypeField> sourceFields = input.getRowType().getFieldList();

        // Replace insert value reference, expressions in SET are at the end of row
        final int offset = sourceFields.size() - parent.getUpdateColumnList().size();
        Ord.zip(updateColumnList).forEach(ord -> columnIndexMap.put(ord.e, offset + updateColumnMapping.get(ord.i)));

        // Build mapping from insert columns to select result
        final List<Integer> valuePermute = targetFields.stream()
            .map(f -> columnIndexMap.get(f.getName()))
            .collect(Collectors.toList());

        final LogicalInsert insert =
            buildInsertOrReplace(parent, targetTable, sourceFields, valuePermute, targetRowType.getFieldNames(),
                Operation.INSERT, null, null, false);

        if (!isBroadCast) {
            return ComplexTaskPlanUtils.canWrite(targetMeta) ?
                new ReplicateDistinctInsertWriter(targetTable, insert, deduplicateMapping, targetMeta) :
                new DistinctInsertWriter(targetTable, insert, deduplicateMapping);
        } else {
            return ComplexTaskPlanUtils.canWrite(targetMeta) ?
                new ReplicateBroadcastInsertWriter(targetTable, insert, deduplicateMapping, tableRule, targetMeta) :
                new BroadcastInsertWriter(targetTable, insert, deduplicateMapping, tableRule);
        }
    }

    public static UpsertWriter createUpsertWriter(LogicalInsert parent, RelOptTable targetTable,
                                                  List<String> updateColumnList,
                                                  List<Integer> updateColumnMapping,
                                                  List<Integer> tarPermute,
                                                  List<String> selectListForDuplicateCheck,
                                                  boolean withoutUpdate,
                                                  TableMeta gsiMeta,
                                                  boolean isGsi,
                                                  boolean isBroadcast,
                                                  boolean isSingle,
                                                  ExecutionContext ec) {
        final RelDataType srcRowType = parent.getInsertRowType();

        // Build insert writer
        final InsertWriter insertWriter =
            createInsertOrReplaceWriter(parent, targetTable, tarPermute, gsiMeta, null, null, false,
                isBroadcast,
                isSingle, ec);
        final InsertWriter distinctInsertWriter =
            createInsertOrReplaceWriter(parent, targetTable, srcRowType, tarPermute, gsiMeta, null, null, false,
                isBroadcast, isSingle, false, ec);

        if (withoutUpdate) {
            // For gsi table contains no column in ON DUPLICATE KEY UPDATE statement
            assert null != gsiMeta;
            return new UpsertGsiWriter(parent, targetTable, insertWriter, distinctInsertWriter, null, null,
                true,
                gsiMeta);
        }

        final MappingBuilder mappingBuilder = MappingBuilder.create(selectListForDuplicateCheck);

        // Build update writer
        final DistinctWriter updateWriter =
            isBroadcast || isSingle ?
                createBroadcastOrSingleUpdateWriter(parent, targetTable, updateColumnList, updateColumnMapping,
                    SqlNodeList.EMPTY, mappingBuilder, isBroadcast, isSingle, ec) :
                createUpdateWriter(parent, targetTable, updateColumnList, updateColumnMapping, SqlNodeList.EMPTY,
                    mappingBuilder, ec);

        if (!isGsi) {
            return new UpsertWriter(parent, targetTable, insertWriter, distinctInsertWriter, updateWriter,
                false);
        } else {
            DistinctWriter deleteWriter = null;
            if (isBroadcast) {
                deleteWriter = createBroadcastDeleteWriter(parent, targetTable, 0, ec);
            } else if (isSingle) {
                deleteWriter = createSingleDeleteWriter(parent, targetTable, 0, ec);
            } else {
                deleteWriter = createDeleteWriter(parent, targetTable, 0, ec);
            }

            return new UpsertGsiWriter(parent, targetTable, insertWriter, distinctInsertWriter, updateWriter,
                deleteWriter, false,
                gsiMeta);
        }
    }

    /**
     * For upsert modifying partition column:
     * 1. Select all columns from source table
     * 2. Update specified column of selected rows
     * 3. Insert with all columns of target table
     */
    public static UpsertRelocateWriter createUpsertRelocateWriter(LogicalInsert parent, RelOptTable targetTable,
                                                                  List<String> updateColumnList,
                                                                  List<Integer> updateSourceMapping,
                                                                  List<Integer> tarPermute,
                                                                  List<String> selectListForDuplicateCheck,
                                                                  TableMeta gsiMeta,
                                                                  boolean isGsi,
                                                                  boolean isBroadcast,
                                                                  boolean isSingleTable,
                                                                  ExecutionContext ec) {
        final RelOptTable primaryTable = parent.getTargetTables().get(0);
        final RelDataType srcRowType = RelOptTableImpl.realRowType(primaryTable);
        final RelDataType oriSrcRowType = parent.getInsertRowType();
        final MappingBuilder mappingBuilder = MappingBuilder.create(selectListForDuplicateCheck);

        // Build delete writer
        DistinctWriter relocateDeleteWriter;
        if (isBroadcast || isSingleTable) {
            relocateDeleteWriter =
                createBroadcastOrSingleDeleteWriter(parent, targetTable, 0, isBroadcast, isSingleTable, ec);
        } else {
            relocateDeleteWriter = createDeleteWriter(parent, targetTable, 0, ec);
        }

        // Build insert writer
        final InsertWriter simpleInsertWriter =
            createInsertOrReplaceWriter(parent, targetTable, tarPermute, gsiMeta, null, null, false,
                isBroadcast,
                isSingleTable, ec);
        final DistinctWriterWrapper relocateInsertWriter = DistinctWriterWrapper.wrap(
            createInsertOrReplaceWriter(parent, targetTable, srcRowType, tarPermute, gsiMeta, null,
                null, false,
                isBroadcast, isSingleTable, false, ec));
        final InsertWriter distinctWriterWrapper =
            createInsertOrReplaceWriter(parent, targetTable, oriSrcRowType, tarPermute, gsiMeta, null,
                null, false,
                isBroadcast, isSingleTable, false, ec);

        // Build update writer
        // For rows of which sharding column will not be modified
        final DistinctWriter updateWriter =
            isBroadcast || isSingleTable ?
                createBroadcastOrSingleUpdateWriter(parent, targetTable, updateColumnList, updateSourceMapping,
                    SqlNodeList.EMPTY, mappingBuilder, isBroadcast, isSingleTable, ec) :
                createUpdateWriter(parent, targetTable, updateColumnList, updateSourceMapping, SqlNodeList.EMPTY,
                    mappingBuilder, ec);

        // Get partition key names
        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
        final OptimizerContext oc = OptimizerContext.getContext(qn.left);
        final TableMeta targetTableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);
        final Set<String> identifierKeyNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        identifierKeyNames.addAll(oc.getRuleManager().getSharedColumns(qn.right));
        final boolean allGsiPublished = !isGsi || GlobalIndexMeta.isPublished(ec, gsiMeta);

        if (!allGsiPublished || ComplexTaskPlanUtils.canWrite(targetTableMeta)) {
            final List<String> pkNames = ImmutableList
                .copyOf(targetTableMeta.getPrimaryKey().stream().map(ColumnMeta::getName)
                    .collect(Collectors.toList()));
            identifierKeyNames.addAll(pkNames);
        }

        final int offset = selectListForDuplicateCheck.size();
        final MappingBuilder updateMappingBuilder = MappingBuilder.create(updateColumnList);

        final Mapping skTargetMapping =
            updateMappingBuilder.targetOrderedSource(identifierKeyNames).buildMapping(offset);
        final Mapping skSourceMapping =
            mappingBuilder.targetOrderedSource(identifierKeyNames).buildMapping();
        final List<ColumnMeta> indentifierMetas =
            mappingBuilder.targetOrderedSource(identifierKeyNames).getSource().stream()
                .map(targetTableMeta::getColumn)
                .collect(Collectors.toList());
        final boolean modifySkOnly = identifierKeyNames.containsAll(updateColumnList);

        if (!isGsi) {
            return new UpsertRelocateWriter(parent, targetTable, simpleInsertWriter,
                distinctWriterWrapper,
                relocateDeleteWriter, relocateInsertWriter, updateWriter, skTargetMapping,
                skSourceMapping,
                indentifierMetas,
                modifySkOnly);
        } else {
            // Gsi cannot be broadcast
            return new UpsertRelocateGsiWriter(parent, targetTable, simpleInsertWriter,
                distinctWriterWrapper,
                relocateDeleteWriter, relocateInsertWriter, updateWriter, skTargetMapping,
                skSourceMapping,
                indentifierMetas,
                modifySkOnly,
                gsiMeta);
        }
    }

    public static ReplaceRelocateWriter createReplaceRelocateWriter(LogicalInsert parent, RelOptTable
        targetTable,
                                                                    List<Integer> valuePermute, TableMeta gsiMeta,
                                                                    boolean containsAllUk,
                                                                    boolean isGsi,
                                                                    boolean isBroadcast,
                                                                    boolean isSingle,
                                                                    ExecutionContext ec) {
        final RelNode input = RelUtils.getRelInput(parent);
        final RelDataType srcRowType = input.getRowType();
        final int fieldCount = srcRowType.getFieldCount();
        final List<String> columnNames = srcRowType.getFieldNames();

        final DistinctWriterWrapper insertWriter =
            DistinctWriterWrapper.wrap(
                createInsertOrReplaceWriter(parent, targetTable, valuePermute, gsiMeta, null, null,
                    false, isBroadcast,
                    isSingle, ec));
        final DistinctWriterWrapper replaceWriter = DistinctWriterWrapper.wrap(
            createInsertOrReplaceWriter(parent, targetTable, valuePermute, gsiMeta, null, null,
                true, isBroadcast,
                isSingle, ec));
        final DistinctWriter deleteWriter =
            isBroadcast || isSingle ?
                createBroadcastOrSingleDeleteWriter(parent, targetTable, 0, isBroadcast, isSingle, ec) :
                createDeleteWriter(parent, targetTable, 0, ec);

        // Build column index map for target table
        final BitSet targetBitSet = BitSets.of(valuePermute);
        final Map<String, Integer> columnIndexMap =
            RelUtils.getColumnIndexMap(targetTable, parent, targetBitSet);

        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
        final OptimizerContext oc = OptimizerContext.getContext(qn.left);
        assert null != oc;
        final TableMeta targetTableMeta = ec.getSchemaManager(qn.left).getTable(qn.right);

        final Set<String> skNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        skNames.addAll(oc.getRuleManager().getSharedColumns(qn.right));

        final Map<String, Integer> skTargetMap = new LinkedHashMap<>();
        final Map<String, Integer> skSourceMap = new LinkedHashMap<>();
        Ord.zip(columnNames).stream().filter(o -> skNames.contains(o.e)).forEach(o -> {
            skSourceMap.put(o.e, o.i);
            skTargetMap.put(o.e, columnIndexMap.get(o.e));
        });

        final Mapping skTargetMapping =
            Mappings.source(ImmutableList.copyOf(skTargetMap.values()), fieldCount);
        final Mapping skSourceMapping =
            Mappings.source(ImmutableList.copyOf(skSourceMap.values()), fieldCount);
        final List<ColumnMeta> skMetas =
            skTargetMap.keySet().stream().map(targetTableMeta::getColumn).collect(
                Collectors.toList());

        if (!isGsi) {
            return new ReplaceRelocateWriter(parent, targetTable, deleteWriter, insertWriter,
                replaceWriter,
                skTargetMapping, skSourceMapping, skMetas, containsAllUk);
        } else {
            return new ReplaceRelocateGsiWriter(parent, targetTable, deleteWriter, insertWriter,
                replaceWriter,
                skTargetMapping, skSourceMapping, skMetas, containsAllUk, gsiMeta);
        }
    }

    public static InsertWriter createInsertOrReplaceWriter(LogicalInsert parent, RelOptTable
        targetTable,
                                                           List<Integer> valuePermute, TableMeta gsiMeta,
                                                           List<String> keyWords, List<RexNode> duplicateKeyUpdateList,
                                                           boolean isReplace, boolean isBroadcast, boolean isSingle,
                                                           ExecutionContext ec) {
        final boolean isValueSource = !parent.isSourceSelect();
        return createInsertOrReplaceWriter(parent, targetTable, parent.getInsertRowType(),
            valuePermute, gsiMeta,
            keyWords, duplicateKeyUpdateList, isReplace, isBroadcast, isSingle, isValueSource, ec);
    }

    /**
     * Create InsertWriter from LogicalDynamicValues
     */
    public static InsertWriter createInsertOrReplaceWriter(LogicalInsert parent, RelOptTable
        targetTable,
                                                           RelDataType sourceRowType, List<Integer> valuePermute,
                                                           TableMeta tableMeta, List<String> keyWords,
                                                           List<RexNode> duplicateKeyUpdateList, boolean isReplace,
                                                           boolean isBroadcast, boolean isSingle,
                                                           boolean isValueSource, ExecutionContext ec) {
        /*
         * Build insert or replace
         */
        final List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        List<String> fieldNames = sourceRowType.getFieldNames();
        if (!isValueSource) {
            fieldNames = valuePermute.stream().map(fieldNames::get).collect(Collectors.toList());
        }

        final LogicalInsert insertOrReplace =
            buildInsertOrReplace(parent, targetTable, sourceFields, valuePermute, fieldNames,
                isReplace ? Operation.REPLACE : Operation.INSERT, keyWords, duplicateKeyUpdateList,
                isValueSource);

        boolean isGsi = tableMeta.isGsi();
        boolean isNeedGenReplicaPlan = ComplexTaskPlanUtils.canWrite(tableMeta);

        InsertWriter insertWriter = null;
        TableRule tableRule =
            OptimizerContext.getContext(tableMeta.getSchemaName())
                .getRuleManager()
                .getTableRule(insertOrReplace.getLogicalTableName());
        if (isBroadcast) {
            insertWriter = isGsi ? (isNeedGenReplicaPlan ?
                new ReplicateBroadcastInsertGsiWriter(targetTable, insertOrReplace, null,
                    tableRule, tableMeta) :
                new BroadcastInsertGsiWriter(targetTable, insertOrReplace, null, tableMeta,
                    tableRule)) : (isNeedGenReplicaPlan ?
                new ReplicateBroadcastInsertWriter(targetTable, insertOrReplace, null, tableRule, tableMeta) :
                new BroadcastInsertWriter(targetTable, insertOrReplace, null, tableRule));
        } else if (isSingle) {
            insertWriter = isGsi ?
                (isNeedGenReplicaPlan ?
                    new ReplicateSingleInsertGsiWriter(targetTable, insertOrReplace, tableMeta, tableRule) :
                    new SingleInsertGsiWriter(targetTable, insertOrReplace, tableMeta, tableRule)) :
                (isNeedGenReplicaPlan ?
                    new ReplicateSingleInsertWriter(targetTable, insertOrReplace, tableMeta, tableRule) :
                    new SingleInsertWriter(targetTable, insertOrReplace, tableMeta, tableRule));
        } else {
            insertWriter = isGsi ?
                (isNeedGenReplicaPlan ? new ReplicationInsertGsiWriter(targetTable, insertOrReplace, tableMeta) :
                    new InsertGsiWriter(targetTable, insertOrReplace, tableMeta)) :
                (isNeedGenReplicaPlan ? new ReplicationInsertWriter(targetTable, insertOrReplace, tableMeta) :
                    new InsertWriter(targetTable, insertOrReplace));
        }
        return insertWriter;
    }

    /**
     * Build single table insert
     *
     * @param parent parent
     * @param targetTable target table
     * @param sourceFields source fields
     * @param valuePermute mapping from insert columns to select result
     * @return insert node
     */
    private static LogicalInsert buildInsertOrReplace(TableModify parent, RelOptTable targetTable,
                                                      List<RelDataTypeField> sourceFields, List<Integer> valuePermute,
                                                      List<String> targetFieldNames, Operation operation,
                                                      List<String> keyWords, List<RexNode> duplicateKeyUpdateList,
                                                      boolean isValueSource) {
        final RexBuilder rexBuilder = parent.getCluster().getRexBuilder();

        ImmutableList<ImmutableList<RexNode>> values = null;
        RelDataType insertRowType = null;
        if (isValueSource) {
            final LogicalDynamicValues input = RelUtils.getRelInput(parent);

            // Build dynamic params
            values = input.getTuples().stream().map(row -> valuePermute
                .stream()
                .map(i -> {
                    RexNode rexNode = row.get(i);
                    if (rexNode instanceof RexDynamicParam) {
                        rexNode = rexBuilder
                            .makeDynamicParam(sourceFields.get(i).getType(),
                                ((RexDynamicParam) rexNode).getIndex());
                    }
                    return rexNode;
                })
                .collect(ImmutableList.toImmutableList())).collect(ImmutableList.toImmutableList());

            // Build insert row type
            insertRowType = RexUtil
                .createOriginalStructType(parent.getCluster().getTypeFactory(), values.get(0),
                    valuePermute.stream().map(targetFieldNames::get).collect(Collectors.toList()));
        } else {
            // Build dynamic params
            final List<RexNode> dynamicParams =
                valuePermute.stream()
                    .map(o -> rexBuilder.makeDynamicParam(sourceFields.get(o).getType(), o))
                    .collect(Collectors.toList());
            values = ImmutableList.of(ImmutableList.copyOf(dynamicParams));

            // Build insert row type
            insertRowType =
                RexUtil
                    .createOriginalStructType(parent.getCluster().getTypeFactory(), dynamicParams,
                        targetFieldNames);
        }

        // Build LogicalDynamicValues
        final LogicalDynamicValues dynamicValues =
            LogicalDynamicValues.createDrdsValues(parent.getCluster(),
                parent.getTraitSet(),
                insertRowType,
                values);

        final TableInfo insertTableInfo = TableInfo.singleSource(targetTable);

        return new LogicalInsert(parent.getCluster(),
            parent.getTraitSet(),
            targetTable,
            parent.getCatalogReader(),
            dynamicValues,
            operation,
            false,
            insertRowType,
            keyWords,
            duplicateKeyUpdateList,
            0,
            null,
            null,
            insertTableInfo);
    }

    private static SqlNodeList getKeywords(SqlNodeList keywords) {
        return new SqlNodeList(keywords
            .getList()
            .stream()
            .filter(kw -> !(kw instanceof SqlLiteral
                && ((SqlLiteral) kw).getTypeName() == SqlTypeName.SYMBOL
                && ((SqlLiteral) kw).symbolValue(SqlDmlKeyword.class) == SqlDmlKeyword.IGNORE))
            .collect(Collectors.toList()), SqlParserPos.ZERO);
    }

    /**
     * Get index columns of primary key or unique key(if pk not exists)
     *
     * @param targetMeta Table meta
     * @param ec
     * @return Primary/Unique keys
     */
    public static String getUniqueConditionColumns(TableMeta targetMeta, List<String> outColumnNames,
                                                   ExecutionContext ec, Supplier errorHandler) {
        final AtomicReference<String> indexName = new AtomicReference<>("");

        boolean pkCondition = false;
        if (targetMeta.isHasPrimaryKey()) {
            // Pk condition
            targetMeta.getPrimaryKey().stream().map(ColumnMeta::getName)
                .forEach(outColumnNames::add);
            indexName.set("PRIMARY");
            pkCondition = true;
        } else if (targetMeta.isGsi()) {
            // For GSI
            final String primarySchemaName = targetMeta.getGsiTableMetaBean().gsiMetaBean.tableSchema;
            final String primaryTableName = targetMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            final TableMeta primaryTableMeta = ec.getSchemaManager(primarySchemaName).getTable(primaryTableName);
            if (primaryTableMeta.isHasPrimaryKey()) {
                // Pk condition without force index primary
                primaryTableMeta.getPrimaryKey().stream().map(ColumnMeta::getName)
                    .forEach(outColumnNames::add);
                pkCondition = true;
            }
        }

        if (!pkCondition) {
            // Uk condition
            targetMeta.getSecondaryIndexes().stream().filter(IndexMeta::isUniqueIndex).findFirst()
                .ifPresent(im -> {
                    indexName.set(im.getPhysicalIndexName());
                    im.getKeyColumns().stream().map(ColumnMeta::getName)
                        .forEach(outColumnNames::add);
                });
        }

        if (outColumnNames.isEmpty()) {
            errorHandler.get();
        }

        return indexName.get();
    }

    private static TableInfo buildUpdateTableInfo(SqlUpdate sqlUpdate, RelOptTable targetTable,
                                                  List<String> updateColumnList) {
        final List<TableInfoNode> sourceTableInfoNodes = ImmutableList
            .of(new TableInfoNode(sqlUpdate.getTargetTable(), sqlUpdate.getAlias(), ImmutableList.of(targetTable)));
        final List<Integer> targetTableIndexes =
            IntStream.range(0, updateColumnList.size()).mapToObj(i -> 0).collect(Collectors.toList());

        return TableInfo
            .create(sqlUpdate.getTargetTable(), sourceTableInfoNodes, targetTableIndexes,
                ImmutableList.of(TableInfo.buildColumnIndexMapFor(targetTable)));
    }

    private static TableInfo buildDeleteTableInfo(SqlDelete sqlDelete, RelOptTable targetTable) {
        return TableInfo.create(sqlDelete.getTargetTable(), ImmutableList
                .of(new TableInfoNode(sqlDelete.getTargetTable(), sqlDelete.getAlias(), ImmutableList.of(targetTable))),
            ImmutableList.of(0), ImmutableList.of(TableInfo.buildColumnIndexMapFor(targetTable)));
    }
}

