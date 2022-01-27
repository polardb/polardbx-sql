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

import java.util.List;
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
    private final List<RelocateWriter> primaryRelocateWriters;
    private final List<DistinctWriter> primaryUpdateWriters;
    private final List<RelocateWriter> gsiRelocateWriters;
    private final List<DistinctWriter> gsiUpdateWriters;

    // Positions of auto_increment columns update columns
    private final List<Integer> autoIncColumns;

    protected LogicalRelocate(LogicalModify update, List<RelocateWriter> primaryRelocateWriters,
                              List<DistinctWriter> primaryUpdateWriters, List<RelocateWriter> gsiRelocateWriters,
                              List<DistinctWriter> gsiUpdateWriters, List<Integer> autoIncColumns) {
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
        this.primaryRelocateWriters = primaryRelocateWriters;
        this.primaryUpdateWriters = primaryUpdateWriters;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.gsiUpdateWriters = gsiUpdateWriters;
        this.schemaName = update.getSchemaName();
        this.autoIncColumns = autoIncColumns;
    }

    public LogicalRelocate(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader,
                           RelNode input, Operation operation, List<String> updateColumnList,
                           List<RexNode> sourceExpressionList, boolean flattened, List<String> keywords, int batchSize,
                           Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfos, String schemaName,
                           List<RelocateWriter> primaryRelocateWriters, List<DistinctWriter> primaryUpdateWriters,
                           List<RelocateWriter> gsiRelocateWriters, List<DistinctWriter> gsiUpdateWriters,
                           List<Integer> autoIncColumns) {
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
        this.primaryRelocateWriters = primaryRelocateWriters;
        this.primaryUpdateWriters = primaryUpdateWriters;
        this.gsiRelocateWriters = gsiRelocateWriters;
        this.gsiUpdateWriters = gsiUpdateWriters;
        this.schemaName = schemaName;
        this.autoIncColumns = autoIncColumns;
    }

    /**
     * Create LogicalRelocate for modifying sharding column of single primary table only
     *
     * @param update Base LogicalModify
     * @param relocateWriters Relocate writers
     * @return LogicalRelocate
     */
    public static LogicalRelocate singleTargetWithoutGsi(LogicalModify update,
                                                         List<RelocateWriter> relocateWriters,
                                                         List<Integer> autoIncColumns) {
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(update.isUpdate());

        // Single-table update
        Preconditions.checkArgument(update.getTableInfo().isSingleTarget());

        return new LogicalRelocate(update, relocateWriters, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
            autoIncColumns);
    }

    public static LogicalRelocate create(LogicalModify update, List<RelocateWriter> primaryRelocateWriters,
                                         List<DistinctWriter> primaryUpdateWriters,
                                         List<RelocateWriter> gsiRelocateWriters,
                                         List<DistinctWriter> gsiUpdateWriters,
                                         List<Integer> autoIncColumns) {
        Preconditions.checkNotNull(update);
        Preconditions.checkArgument(update.isUpdate());

        return new LogicalRelocate(update, primaryRelocateWriters, primaryUpdateWriters, gsiRelocateWriters,
            gsiUpdateWriters, autoIncColumns);
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

        final List<String> relocateSet = Stream.concat(primaryRelocateWriters.stream(),
            gsiRelocateWriters.stream())
            .map(w -> Util.last(w.getTargetTable().getQualifiedName()))
            .collect(Collectors.toList());

        if (!relocateSet.isEmpty()) {
            pw.item("RELOCATE", String.join(", ", relocateSet));
        }

        final List<String> updateSet = Stream.concat(primaryUpdateWriters.stream(),
            gsiUpdateWriters.stream())
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
            getPrimaryRelocateWriters(),
            getPrimaryUpdateWriters(),
            getGsiRelocateWriters(),
            getGsiUpdateWriters(),
            getAutoIncColumns());
    }

    public List<RelocateWriter> getPrimaryRelocateWriters() {
        return primaryRelocateWriters;
    }

    public List<DistinctWriter> getPrimaryUpdateWriters() {
        return primaryUpdateWriters;
    }

    public List<RelocateWriter> getGsiRelocateWriters() {
        return gsiRelocateWriters;
    }

    public List<DistinctWriter> getGsiUpdateWriters() {
        return gsiUpdateWriters;
    }

    public List<Integer> getAutoIncColumns() {
        return autoIncColumns;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }
}
