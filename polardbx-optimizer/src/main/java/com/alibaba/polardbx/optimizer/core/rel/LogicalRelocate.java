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
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Modify sharding key
 *
 * @author chenmo.cm
 */
public class LogicalRelocate extends TableModify {

    private final String schemaName;

    // Positions of auto_increment columns update columns
    private final List<Integer> autoIncColumns;

    // Writers group by target table
    private final Map<Integer, List<RelocateWriter>> relocateWriterMap;
    private final Map<Integer, List<DistinctWriter>> modifyWriterMap;

    // Source columns and target columns group by target table
    private final Map<Integer, Mapping> setColumnTargetMappings;
    private final Map<Integer, Mapping> setColumnSourceMappings;
    private final Map<Integer, List<ColumnMeta>> setColumnMetas;

    // Primary writer
    private final Map<Integer, DistinctWriter> primaryDistinctWriter;
    private final Map<Integer, RelocateWriter> primaryRelocateWriter;

    protected LogicalRelocate(LogicalModify update, List<Integer> autoIncColumns,
                              Map<Integer, List<RelocateWriter>> relocateWriterMap,
                              Map<Integer, List<DistinctWriter>> modifyWriterMap,
                              Map<Integer, Mapping> setColumnTargetMappings,
                              Map<Integer, Mapping> setColumnSourceMappings,
                              Map<Integer, List<ColumnMeta>> setColumnMetas,
                              Map<Integer, DistinctWriter> primaryDistinctWriter,
                              Map<Integer, RelocateWriter> primaryRelocateWriter) {
        super(update.getCluster(),
            update.getTraitSet(),
            update.getTable(),
            update.getCatalogReader(),
            update.getInput(),
            update.getOperation(),
            update.getUpdateColumnList(),
            update.getSourceExpressionList(),
            update.isFlattened(),
            update.getKeywords(),
            update.getBatchSize(),
            update.getAppendedColumnIndex(),
            update.getHints(),
            update.getTableInfo());
        this.schemaName = update.getSchemaName();
        this.autoIncColumns = autoIncColumns;
        this.relocateWriterMap = relocateWriterMap;
        this.modifyWriterMap = modifyWriterMap;
        this.setColumnTargetMappings = setColumnTargetMappings;
        this.setColumnSourceMappings = setColumnSourceMappings;
        this.setColumnMetas = setColumnMetas;
        this.primaryDistinctWriter = primaryDistinctWriter;
        this.primaryRelocateWriter = primaryRelocateWriter;
    }

    public LogicalRelocate(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader,
                           RelNode input, Operation operation, List<String> updateColumnList,
                           List<RexNode> sourceExpressionList, boolean flattened, List<String> keywords, int batchSize,
                           Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfos, String schemaName,
                           List<Integer> autoIncColumns, Map<Integer, List<RelocateWriter>> relocateWriterMap,
                           Map<Integer, List<DistinctWriter>> modifyWriterMap,
                           Map<Integer, Mapping> setColumnTargetMappings, Map<Integer, Mapping> setColumnSourceMappings,
                           Map<Integer, List<ColumnMeta>> setColumnMetas,
                           Map<Integer, DistinctWriter> primaryDistinctWriter,
                           Map<Integer, RelocateWriter> primaryRelocateWriter) {
        super(cluster,
            traitSet,
            table,
            catalogReader,
            input,
            operation,
            updateColumnList,
            sourceExpressionList,
            flattened,
            keywords,
            batchSize,
            appendedColumnIndex,
            hints,
            tableInfos);
        this.schemaName = schemaName;
        this.autoIncColumns = autoIncColumns;
        this.relocateWriterMap = relocateWriterMap;
        this.modifyWriterMap = modifyWriterMap;
        this.setColumnTargetMappings = setColumnTargetMappings;
        this.setColumnSourceMappings = setColumnSourceMappings;
        this.setColumnMetas = setColumnMetas;
        this.primaryDistinctWriter = primaryDistinctWriter;
        this.primaryRelocateWriter = primaryRelocateWriter;
    }

    /**
     * Create LogicalRelocate for modifying sharding column of single primary table only
     *
     * @param update Base LogicalModify
     * @return LogicalRelocate
     */
    public static LogicalRelocate singleTargetWithoutGsi(LogicalModify update,
                                                         List<Integer> autoIncColumns,
                                                         Map<Integer, List<RelocateWriter>> relocateWriterMap,
                                                         Map<Integer, List<DistinctWriter>> modifyWriterMap,
                                                         Map<Integer, Mapping> setColumnTargetMappings,
                                                         Map<Integer, Mapping> setColumnSourceMappings,
                                                         Map<Integer, List<ColumnMeta>> setColumnMetas,
                                                         Map<Integer, DistinctWriter> primaryDistinctWriter,
                                                         Map<Integer, RelocateWriter> primaryRelocateWriter) {
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(update.isUpdate());

        // Single-table update
        Preconditions.checkArgument(update.getTableInfo().isSingleTarget());

        return new LogicalRelocate(update, autoIncColumns, relocateWriterMap, modifyWriterMap, setColumnTargetMappings,
            setColumnSourceMappings, setColumnMetas, primaryDistinctWriter, primaryRelocateWriter);
    }

    public static LogicalRelocate create(LogicalModify update,
                                         List<Integer> autoIncColumns,
                                         Map<Integer, List<RelocateWriter>> relocateWriterMap,
                                         Map<Integer, List<DistinctWriter>> modifyWriterMap,
                                         Map<Integer, Mapping> setColumnTargetMappings,
                                         Map<Integer, Mapping> setColumnSourceMappings,
                                         Map<Integer, List<ColumnMeta>> setColumnMetas,
                                         Map<Integer, DistinctWriter> primaryDistinctWriter,
                                         Map<Integer, RelocateWriter> primaryRelocateWriter) {
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(update.isUpdate());

        return new LogicalRelocate(update, autoIncColumns, relocateWriterMap,
            modifyWriterMap, setColumnTargetMappings, setColumnSourceMappings, setColumnMetas, primaryDistinctWriter,
            primaryRelocateWriter);
    }

    protected String explainNodeName() {
        return "LogicalRelocate";
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
        pw.item("TYPE", getOperation());

        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < getUpdateColumnList().size(); i++) {
            stringBuilder.append(getTargetTableNames().get(i));
            stringBuilder.append(".");
            stringBuilder.append(getUpdateColumnList().get(i));
            stringBuilder.append("=");
            stringBuilder.append(getSourceExpressionList().get(i));
            if (i < getUpdateColumnList().size() - 1) {
                stringBuilder.append(", ");
            }
        }
        pw.item("SET", stringBuilder.toString());

        final List<String> relocateSet = relocateWriterMap.values().stream().flatMap(Collection::stream)
            .map(w -> Util.last(w.getTargetTable().getQualifiedName()))
            .collect(Collectors.toList());

        if (!relocateSet.isEmpty()) {
            pw.item("RELOCATE", String.join(", ", relocateSet));
        }

        final List<String> updateSet = modifyWriterMap.values().stream().flatMap(Collection::stream)
            .map(w -> Util.last(w.getTargetTable().getQualifiedName()))
            .collect(Collectors.toList());

        if (!updateSet.isEmpty()) {
            pw.item("UPDATE", String.join(", ", updateSet));
        }
        return pw;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalRelocate(getCluster(),
            traitSet,
            getTable(),
            getCatalogReader(),
            sole(inputs),
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList(),
            isFlattened(),
            getKeywords(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getSchemaName(),
            getAutoIncColumns(),
            getRelocateWriterMap(),
            getModifyWriterMap(),
            getSetColumnTargetMappings(),
            getSetColumnSourceMappings(),
            getSetColumnMetas(),
            getPrimaryDistinctWriter(),
            getPrimaryRelocateWriter());
    }

    public List<Integer> getAutoIncColumns() {
        return autoIncColumns;
    }

    public Map<Integer, List<RelocateWriter>> getRelocateWriterMap() {
        return relocateWriterMap;
    }

    public Map<Integer, List<DistinctWriter>> getModifyWriterMap() {
        return modifyWriterMap;
    }

    public Map<Integer, Mapping> getSetColumnTargetMappings() {
        return setColumnTargetMappings;
    }

    public Map<Integer, Mapping> getSetColumnSourceMappings() {
        return setColumnSourceMappings;
    }

    public Map<Integer, List<ColumnMeta>> getSetColumnMetas() {
        return setColumnMetas;
    }

    public Map<Integer, DistinctWriter> getPrimaryDistinctWriter() {
        return primaryDistinctWriter;
    }

    public Map<Integer, RelocateWriter> getPrimaryRelocateWriter() {
        return primaryRelocateWriter;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }
}
