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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadCastReplaceScaleOutWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
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
import java.util.Optional;
import java.util.Set;

/**
 * REPLACE on sharding table
 *
 * @author chenmo.cm
 */
public class LogicalReplace extends LogicalInsertIgnore {

    private final ReplaceRelocateWriter primaryRelocateWriter;
    private final List<ReplaceRelocateWriter> gsiRelocateWriters;
    private final BroadCastReplaceScaleOutWriter broadCastReplaceScaleOutWriter;

    public LogicalReplace(LogicalInsert insert,
                          InsertWriter primaryInsertWriter,
                          ReplaceRelocateWriter primaryRelocateWriter,
                          List<InsertWriter> gsiInsertWriters,
                          List<ReplaceRelocateWriter> gsiRelocateWriters,
                          BroadCastReplaceScaleOutWriter broadCastReplaceScaleOutWriter,
                          List<String> selectListForDuplicateCheck
    ) {
        super(insert.getCluster(),
            insert.getTraitSet(),
            insert.getTable(),
            insert.getCatalogReader(),
            insert.getInput(),
            Operation.REPLACE,
            insert.isFlattened(),
            insert.getInsertRowType(),
            insert.getKeywords(),
            ImmutableList.of(),
            insert.getBatchSize(),
            insert.getAppendedColumnIndex(),
            insert.getHints(),
            insert.getTableInfo(),
            insert.getPrimaryInsertWriter(),
            insert.getGsiInsertWriters(),
            insert.getAutoIncParamIndex(),
            selectListForDuplicateCheck,
            initColumnMeta(insert),
            initTableColumnMeta(insert),
            insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList(),
            insert.getPushDownInsertWriter(),
            insert.getGsiInsertIgnoreWriters(),
            insert.getPrimaryDeleteWriter(),
            insert.getGsiDeleteWriters()
        );
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.primaryInsertWriter = primaryInsertWriter;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.gsiInsertWriters = gsiInsertWriters;
        this.broadCastReplaceScaleOutWriter = broadCastReplaceScaleOutWriter;
    }

    protected LogicalReplace(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                             Prepare.CatalogReader catalogReader, RelNode input, Operation operation, boolean flattened,
                             RelDataType insertRowType, List<String> keywords, List<RexNode> duplicateKeyUpdateList,
                             int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfo,
                             InsertWriter primaryInsertWriter, List<InsertWriter> gsiInsertWriters,
                             List<Integer> autoIncParamIndex, List<List<String>> ukColumnNamesList,
                             List<List<Integer>> beforeUkMapping, List<List<Integer>> afterUkMapping,
                             List<String> pkColumnNames, List<Integer> beforePkMapping, List<Integer> afterPkMapping,
                             Set<String> allUkSet, Map<String, Map<String, Set<String>>> tableUkMap,
                             List<ColumnMeta> rowColumnMetas, List<ColumnMeta> tableColumnMetas,
                             List<String> selectListForDuplicateCheck, ReplaceRelocateWriter primaryRelocateWriter,
                             List<ReplaceRelocateWriter> gsiRelocateWriters,
                             BroadCastReplaceScaleOutWriter broadCastReplaceScaleOutWriter,
                             boolean targetTableIsWritable, boolean targetTableIsReadyToPublish,
                             boolean sourceTablesIsReadyToPublish, LogicalDynamicValues logicalDynamicValues,
                             List<RexNode> unOpitimizedDuplicateKeyUpdateList, InsertWriter pushDownInsertWriter,
                             List<InsertWriter> gsiInsertIgnoreWriter, DistinctWriter primaryDeleteWriter,
                             List<DistinctWriter> gsiDeleteWriters) {
        super(cluster, traitSet, table, catalogReader, input, operation, flattened, insertRowType, keywords,
            duplicateKeyUpdateList, batchSize, appendedColumnIndex, hints, tableInfo, primaryInsertWriter,
            gsiInsertWriters, autoIncParamIndex, ukColumnNamesList, beforeUkMapping, afterUkMapping, pkColumnNames,
            beforePkMapping, afterPkMapping, allUkSet, tableUkMap, rowColumnMetas, tableColumnMetas,
            selectListForDuplicateCheck, targetTableIsWritable, targetTableIsReadyToPublish,
            sourceTablesIsReadyToPublish, logicalDynamicValues, unOpitimizedDuplicateKeyUpdateList,
            pushDownInsertWriter, gsiInsertIgnoreWriter, primaryDeleteWriter, gsiDeleteWriters);
        this.primaryRelocateWriter = primaryRelocateWriter;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.broadCastReplaceScaleOutWriter = broadCastReplaceScaleOutWriter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        final LogicalReplace newLogicalReplace = new LogicalReplace(getCluster(),
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
            getPkColumnNames(),
            getBeforePkMapping(),
            getAfterPkMapping(),
            getAllUkSet(),
            getTableUkMap(),
            getRowColumnMetaList(),
            getTableColumnMetaList(),
            getSelectListForDuplicateCheck(),
            getPrimaryRelocateWriter(),
            getGsiRelocateWriters(),
            getBroadCastReplaceScaleOutWriter(),
            isTargetTableIsWritable(),
            isTargetTableIsReadyToPublish(),
            isSourceTablesIsReadyToPublish(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getPushDownInsertWriter(),
            getGsiInsertIgnoreWriters(),
            getPrimaryDeleteWriter(),
            getGsiDeleteWriters());
        return newLogicalReplace;
    }

    public ReplaceRelocateWriter getPrimaryRelocateWriter() {
        return primaryRelocateWriter;
    }

    public List<ReplaceRelocateWriter> getGsiRelocateWriters() {
        return gsiRelocateWriters;
    }

    public BroadCastReplaceScaleOutWriter getBroadCastReplaceScaleOutWriter() {
        return broadCastReplaceScaleOutWriter;
    }

    @Override
    protected <R extends LogicalInsert> List<RelNode> getPhyPlanForDisplay(ExecutionContext executionContext,
                                                                           R replace) {
        final InsertWriter primaryWriter = replace.getPrimaryInsertWriter();
        final LogicalInsert insert = primaryWriter.getInsert();
        final LogicalInsert copied = new LogicalInsert(insert.getCluster(), insert.getTraitSet(), insert.getTable(),
            insert.getCatalogReader(), insert.getInput(), Operation.REPLACE, insert.isFlattened(),
            insert.getInsertRowType(), insert.getKeywords(), insert.getDuplicateKeyUpdateList(),
            insert.getBatchSize(), insert.getAppendedColumnIndex(), insert.getHints(), insert.getTableInfo(), null,
            new ArrayList<>(), insert.getAutoIncParamIndex(), insert.getUnOptimizedLogicalDynamicValues(),
            insert.getUnOptimizedDuplicateKeyUpdateList());

        final InsertWriter replaceWriter = new InsertWriter(primaryWriter.getTargetTable(), copied);
        return replaceWriter.getInput(executionContext);
    }

    @Override
    public InsertWriter getPrimaryInsertWriter() {
        return Optional.ofNullable(this.primaryInsertWriter)
            .orElseGet(() -> getPrimaryRelocateWriter().getModifyWriter().unwrap(InsertWriter.class));
    }
}
