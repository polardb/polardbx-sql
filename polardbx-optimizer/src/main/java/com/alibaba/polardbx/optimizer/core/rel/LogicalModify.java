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

import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * UPDATE / DELETE that cannot be pushed down.
 */
public class LogicalModify extends TableModify {

    private final DbType dbType;
    private SqlNode originalSqlNode;
    private String schemaName = null;

    private final List<RelOptTable> extraTargetTables;
    private final List<String> extraTargetColumns;

    private List<DistinctWriter> primaryModifyWriters = new ArrayList<>();
    private List<DistinctWriter> gsiModifyWriters = new ArrayList<>();
    /**
     * True if any target table contains no primary key
     */
    private boolean withoutPk = false;

    public LogicalModify(TableModify modify) {
        this(modify.getCluster(),
            modify.getTraitSet(),
            modify.getTable(),
            modify.getCatalogReader(),
            modify.getInput(),
            modify.getOperation(),
            modify.getUpdateColumnList(),
            modify.getSourceExpressionList(),
            modify.isFlattened(),
            modify.getKeywords(),
            modify.getHints(),
            modify.getHintContext(),
            modify.getTableInfo(),
            modify instanceof LogicalTableModify ? ((LogicalTableModify) modify).getExtraTargetTables() : ImmutableList
                .of(),
            modify instanceof LogicalTableModify ? ((LogicalTableModify) modify).getExtraTargetColumns() : ImmutableList
                .of(),
            modify instanceof LogicalModify ? ((LogicalModify) modify).getPrimaryModifyWriters() : ImmutableList.of(),
            modify instanceof LogicalModify ? ((LogicalModify) modify).getGsiModifyWriters() : ImmutableList.of(),
            modify instanceof LogicalModify ? ((LogicalModify) modify).isWithoutPk() : false);
    }

    public LogicalModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                         Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
                         List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened,
                         List<String> keywords, SqlNodeList hints, OptimizerHint hintContext, TableInfo tableInfo) {
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
            0,
            null,
            hints,
            hintContext,
            tableInfo);
        this.schemaName = table.getQualifiedName().size() == 2 ? table.getQualifiedName().get(0) : null;
        this.dbType = DbType.MYSQL;
        this.extraTargetTables = ImmutableList.of();
        this.extraTargetColumns = ImmutableList.of();
    }

    public LogicalModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                         Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
                         List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened,
                         List<String> keywords, SqlNodeList hints, OptimizerHint hintContext, TableInfo tableInfo,
                         List<RelOptTable> extraTargetTables, List<String> extraTargetColumns,
                         List<DistinctWriter> primaryModifyWriters, List<DistinctWriter> gsiModifyWriters,
                         boolean withoutPk) {
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
            0,
            null,
            hints,
            hintContext,
            tableInfo);
        this.schemaName = table.getQualifiedName().size() == 2 ? table.getQualifiedName().get(0) : null;
        this.dbType = DbType.MYSQL;
        this.extraTargetTables = extraTargetTables;
        this.extraTargetColumns = extraTargetColumns;
        this.primaryModifyWriters = primaryModifyWriters;
        this.gsiModifyWriters = gsiModifyWriters;
        this.withoutPk = withoutPk;
    }

    public List<String> getTableNames() {
        List<String> tableNames = new ArrayList<>(tables.size());
        for (RelOptTable table : tables) {
            List<String> qualifiedName = table.getQualifiedName();
            String tableName = Util.last(qualifiedName);
            tableNames.add(tableName);
        }
        return tableNames;
    }

    public String getLogicalTableName() {
        return getTargetTableNames().get(0);
    }

    public SqlNode getOriginalSqlNode() {
        return originalSqlNode;
    }

    public void setOriginalSqlNode(SqlNode originalSqlNode) {
        this.originalSqlNode = originalSqlNode;
    }

    public DbType getDbType() {
        return dbType;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
        if (isUpdate()) {
            pw.item("TYPE", "UPDATE");
            StringBuilder stringBuilder = new StringBuilder();
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
        } else {
            pw.item("TYPE", "DELETE");
            pw.item("TABLES",
                getTargetTables().stream()
                    .map(t -> String.join(".", t.getQualifiedName()))
                    .collect(Collectors.joining(", ")));
        }
        return pw;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        LogicalModify logicalModify = new LogicalModify(getCluster(),
            traitSet,
            table,
            catalogReader,
            sole(inputs),
            getOperation(),
            getUpdateColumnList(),
            getSourceExpressionList(),
            isFlattened(),
            getKeywords(),
            getHints(),
            getHintContext(),
            getTableInfo(),
            getExtraTargetTables(),
            getExtraTargetColumns(),
            getPrimaryModifyWriters(),
            getGsiModifyWriters(),
            isWithoutPk());
        logicalModify.originalSqlNode = originalSqlNode;
        return logicalModify;
    }

    public String explainNodeName() {
        return "LogicalModify";
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public List<RelOptTable> getExtraTargetTables() {
        return extraTargetTables;
    }

    public List<String> getExtraTargetColumns() {
        return extraTargetColumns;
    }

    public List<DistinctWriter> getPrimaryModifyWriters() {
        return primaryModifyWriters;
    }

    public void setPrimaryModifyWriters(List<DistinctWriter> primaryModifyWriters) {
        this.primaryModifyWriters = primaryModifyWriters;
    }

    public List<DistinctWriter> getGsiModifyWriters() {
        return gsiModifyWriters;
    }

    public void setGsiModifyWriters(List<DistinctWriter> gsiModifyWriters) {
        this.gsiModifyWriters = gsiModifyWriters;
    }

    public boolean isWithoutPk() {
        return withoutPk;
    }

    public void setWithoutPk(boolean withoutPk) {
        this.withoutPk = withoutPk;
    }
}
