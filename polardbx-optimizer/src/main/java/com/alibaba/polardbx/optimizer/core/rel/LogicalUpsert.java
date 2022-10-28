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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertRelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.UpsertWriter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * INSERT ON DUPLICATE KEY UPDATE on sharding table
 *
 * @author chenmo.cm
 */
public class LogicalUpsert extends LogicalInsertIgnore {

    private final boolean modifyPartitionKey;
    private final boolean modifyUniqueKey;
    /**
     * TRUE if exists direct column reference in DUPLICATE KEY UPDATE list.
     * <p>
     * .e.g: INSERT INTO t (a, b) VALUES(1, 2) ON DUPLICATE KEY UPDATE c = a
     */
    private final boolean withBeforeValueRef;

    /**
     * Mapping from target columns of ON DUPLICATE KEY UPDATE to columns of SELECT statement for duplicate checking
     */
    private final List<Integer> beforeUpdateMapping;

    private final UpsertWriter primaryUpsertWriter;
    private final List<UpsertWriter> gsiUpsertWriters;
    private final RelocateWriter primaryRelocateWriter;
    private final List<RelocateWriter> gsiRelocateWriters;

    private final int rowNumberColumnIndex;

    // If all sharding columns in update list referencing same column in after value
    // e.g. insert into t1(a,b,c) values (1,2,3) on duplicate key update a=values(a),b=values(b),c=values(c)
    private final boolean allUpdatedSkRefValue;

    public LogicalUpsert(LogicalInsert insert,
                         InsertWriter primaryInsertWriter,
                         UpsertWriter primaryUpsertWriter,
                         RelocateWriter primaryRelocateWriter,
                         List<InsertWriter> gsiInsertWriters,
                         List<UpsertWriter> gsiUpsertWriters,
                         List<RelocateWriter> gsiRelocateWriters,
                         List<String> selectListForDuplicateCheck,
                         List<Integer> beforeUpdateMapping,
                         int rowNumberColumnIndex,
                         boolean modifyPartitionKey,
                         boolean modifyUniqueKey,
                         boolean withBeforeValueRef,
                         boolean allUpdatedSkRefValue) {
        super(insert, selectListForDuplicateCheck);
        this.beforeUpdateMapping = beforeUpdateMapping;
        this.rowNumberColumnIndex = rowNumberColumnIndex;
        this.modifyPartitionKey = modifyPartitionKey;
        this.modifyUniqueKey = modifyUniqueKey;
        this.withBeforeValueRef = withBeforeValueRef;
        this.duplicateKeyUpdateList = insert.getDuplicateKeyUpdateList();
        this.primaryUpsertWriter = primaryUpsertWriter;
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.primaryInsertWriter = primaryInsertWriter;
        this.gsiUpsertWriters = gsiUpsertWriters;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.gsiInsertWriters = gsiInsertWriters;
        this.allUpdatedSkRefValue = allUpdatedSkRefValue;
    }

    public LogicalUpsert(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                         Prepare.CatalogReader catalogReader, RelNode input, Operation operation, boolean flattened,
                         RelDataType insertRowType, List<String> keywords, List<RexNode> duplicateKeyUpdateList,
                         int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfo,
                         InsertWriter primaryInsertWriter, List<InsertWriter> gsiInsertWriters,
                         List<Integer> autoIncParamIndex, List<List<String>> ukColumnNamesList,
                         List<List<Integer>> beforeUkMapping, List<List<Integer>> afterUkMapping,
                         List<Integer> selectInsertRowMapping, List<String> pkColumnNames,
                         List<Integer> beforePkMapping, List<Integer> afterPkMapping, Set<String> allUkSet,
                         Map<String, Map<String, Set<String>>> tableUkMap,
                         Map<String, List<List<String>>> ukGroupByTable, Map<String, List<String>> localIndexPhyName,
                         List<ColumnMeta> rowColumnMetas, List<ColumnMeta> tableColumnMetas,
                         List<String> selectListForDuplicateCheck, UpsertWriter primaryUpsertWriter,
                         List<UpsertWriter> gsiUpsertWriters, RelocateWriter primaryRelocateWriter,
                         List<RelocateWriter> gsiRelocateWriters, List<Integer> beforeUpdateMapping,
                         int rowNumberColumnIndex, boolean modifyPartitionKey, boolean modifyUniqueKey,
                         boolean withBeforeValueRef, boolean targetTableIsWritable, boolean targetTableIsReadyToPublish,
                         boolean sourceTablesIsReadyToPublish, LogicalDynamicValues logicalDynamicValues,
                         List<RexNode> unOptimizedDuplicateKeyUpdateList, InsertWriter pushDownInsertWriter,
                         List<InsertWriter> gsiInsertIgnoreWriters, DistinctWriter primaryDeleteWriter,
                         List<DistinctWriter> gsiDeleteWriters, boolean allUpdatedSkRefValue,
                         boolean usePartFieldChecker,
                         Map<String, ColumnMeta> columnMetaMap) {
        super(cluster, traitSet, table, catalogReader, input, operation, flattened, insertRowType, keywords,
            duplicateKeyUpdateList, batchSize, appendedColumnIndex, hints, tableInfo, primaryInsertWriter,
            gsiInsertWriters, autoIncParamIndex, ukColumnNamesList, beforeUkMapping, afterUkMapping,
            selectInsertRowMapping, pkColumnNames, beforePkMapping, afterPkMapping, allUkSet, tableUkMap,
            ukGroupByTable, localIndexPhyName, rowColumnMetas, tableColumnMetas, selectListForDuplicateCheck,
            targetTableIsWritable, targetTableIsReadyToPublish, sourceTablesIsReadyToPublish, logicalDynamicValues,
            unOptimizedDuplicateKeyUpdateList, pushDownInsertWriter, gsiInsertIgnoreWriters, primaryDeleteWriter,
            gsiDeleteWriters, usePartFieldChecker, columnMetaMap);
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.primaryUpsertWriter = primaryUpsertWriter;
        this.gsiUpsertWriters = gsiUpsertWriters;
        this.beforeUpdateMapping = beforeUpdateMapping;
        this.rowNumberColumnIndex = rowNumberColumnIndex;
        this.modifyPartitionKey = modifyPartitionKey;
        this.modifyUniqueKey = modifyUniqueKey;
        this.withBeforeValueRef = withBeforeValueRef;
        this.allUpdatedSkRefValue = allUpdatedSkRefValue;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        final LogicalUpsert newLogicalUpsert = new LogicalUpsert(getCluster(),
            traitSet,
            table,
            catalogReader,
            sole(inputs),
            getOperation(),
            isFlattened(),
            getInsertRowType(),
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getPrimaryInsertWriter(),
            getGsiInsertWriters(),
            getAutoIncParamIndex(),
            getUkColumnNamesList(),
            getBeforeUkMapping(),
            getAfterUkMapping(),
            getSelectInsertColumnMapping(),
            getPkColumnNames(),
            getBeforePkMapping(),
            getAfterPkMapping(),
            getAllUkSet(),
            getTableUkMap(),
            getUkGroupByTable(),
            getLocalIndexPhyName(),
            getRowColumnMetaList(),
            getTableColumnMetaList(),
            getSelectListForDuplicateCheck(),
            getPrimaryUpsertWriter(),
            getGsiUpsertWriters(),
            getPrimaryRelocateWriter(),
            getGsiRelocateWriters(),
            getBeforeUpdateMapping(),
            getRowNumberColumnIndex(),
            isModifyPartitionKey(),
            isModifyUniqueKey(),
            isWithBeforeValueRef(),
            isTargetTableIsWritable(),
            isTargetTableIsReadyToPublish(),
            isSourceTablesIsReadyToPublish(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getPushDownInsertWriter(),
            getGsiInsertIgnoreWriters(),
            getPrimaryDeleteWriter(),
            getGsiDeleteWriters(),
            isAllUpdatedSkRefValue(),
            isUsePartFieldChecker(),
            getColumnMetaMap());
        return newLogicalUpsert;
    }

    public RelocateWriter getPrimaryRelocateWriter() {
        return primaryRelocateWriter;
    }

    public List<RelocateWriter> getGsiRelocateWriters() {
        return gsiRelocateWriters;
    }

    public UpsertWriter getPrimaryUpsertWriter() {
        return primaryUpsertWriter;
    }

    public List<UpsertWriter> getGsiUpsertWriters() {
        return gsiUpsertWriters;
    }

    public boolean isModifyPartitionKey() {
        return modifyPartitionKey;
    }

    public boolean isModifyUniqueKey() {
        return modifyUniqueKey;
    }

    public List<Integer> getBeforeUpdateMapping() {
        return beforeUpdateMapping;
    }

    public int getRowNumberColumnIndex() {
        return rowNumberColumnIndex;
    }

    public boolean isWithBeforeValueRef() {
        return withBeforeValueRef;
    }

    public boolean isAllUpdatedSkRefValue() {
        return allUpdatedSkRefValue;
    }

    @Override
    protected <R extends LogicalInsert> List<RelNode> getPhyPlanForDisplay(ExecutionContext executionContext,
                                                                           R upsert) {
        final InsertWriter primaryWriter = getPrimaryInsertWriter();
        final LogicalInsert insert = primaryWriter.getInsert();
        final LogicalInsert copied = new LogicalInsert(insert.getCluster(), insert.getTraitSet(), insert.getTable(),
            insert.getCatalogReader(), insert.getInput(), Operation.INSERT, insert.isFlattened(),
            insert.getInsertRowType(), insert.getKeywords(), upsert.getDuplicateKeyUpdateList(),
            insert.getBatchSize(), insert.getAppendedColumnIndex(), insert.getHints(), insert.getTableInfo(), null,
            new ArrayList<>(), insert.getAutoIncParamIndex(), insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList());

        final InsertWriter upsertWriter = new InsertWriter(primaryWriter.getTargetTable(), copied);
        return upsertWriter.getInput(executionContext);
    }

    @Override
    public InsertWriter getPrimaryInsertWriter() {
        if (null != this.primaryInsertWriter) {
            return this.primaryInsertWriter;
        } else if (null != this.primaryUpsertWriter) {
            return this.primaryUpsertWriter.getInsertWriter();
        } else {
            return this.primaryRelocateWriter.unwrap(UpsertRelocateWriter.class).getSimpleInsertWriter();
        }
    }
}
