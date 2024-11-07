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
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.utils.RelUtils.LogicalModifyViewBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // Following variables used by generated columns
    private Map<Integer, List<ColumnMeta>> evalRowColumnMetas;
    private Map<Integer, List<Integer>> inputToEvalFieldMappings;
    private Map<Integer, List<RexNode>> genColRexNodes;

    // 需要比较set的行是否相同的变量
    // ON UPDATE TIMESTAMP 列，如果行相同，则无需执行，保证该列值不变
    // {writer，表下标}
    private Map<DistinctWriter, Integer> needCompareWriters;
    // {表下标，对应变量}
    private Map<Integer, Mapping> setColumnTargetMappings;
    private Map<Integer, Mapping> setColumnSourceMappings;
    private Map<Integer, List<ColumnMeta>> setColumnMetas;

    private boolean modifyForeignKey = false;

    @Getter
    @Setter
    protected ModifyTopNInfo modifyTopNInfo = ModifyTopNInfo.EMPTY;

    @Getter
    @Setter
    protected LogicalMultiWriteInfo multiWriteInfo = LogicalMultiWriteInfo.EMPTY;

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
            modify instanceof LogicalModify ? ((LogicalModify) modify).isWithoutPk() : false,
            modify instanceof LogicalModify ? ((LogicalModify) modify).isModifyForeignKey() : false,
            modify instanceof LogicalModify ? ((LogicalModify) modify).getModifyTopNInfo() : ModifyTopNInfo.EMPTY,
            modify instanceof LogicalModify ? ((LogicalModify) modify).getMultiWriteInfo() :
                LogicalMultiWriteInfo.EMPTY);
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
        this.needCompareWriters = new HashMap<>();
        this.setColumnTargetMappings = new HashMap<>();
        this.setColumnSourceMappings = new HashMap<>();
        this.setColumnMetas = new HashMap<>();

    }

    public LogicalModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                         Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
                         List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened,
                         List<String> keywords, SqlNodeList hints, OptimizerHint hintContext, TableInfo tableInfo,
                         List<RelOptTable> extraTargetTables, List<String> extraTargetColumns,
                         List<DistinctWriter> primaryModifyWriters, List<DistinctWriter> gsiModifyWriters,
                         boolean withoutPk, boolean modifyForeignKey, ModifyTopNInfo modifyTopNInfo,
                         LogicalMultiWriteInfo multiWriteInfo) {
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
        this.modifyForeignKey = modifyForeignKey;
        this.needCompareWriters = new HashMap<>();
        this.setColumnTargetMappings = new HashMap<>();
        this.setColumnSourceMappings = new HashMap<>();
        this.setColumnMetas = new HashMap<>();
        this.modifyTopNInfo = modifyTopNInfo;
        this.multiWriteInfo = multiWriteInfo;
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
        if (isModifyTopN()) {
            pw.item("isModifyTopN", true);
        }
        if (isMultiWriteCanBeOptimizedByReturning()) {
            if (this.multiWriteInfo.isOptimizeByReturning()) {
                pw.item("optimizeByReturning", true);
            } else {
                pw.item("canUseReturning", true);
            }
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
            isWithoutPk(),
            isModifyForeignKey(),
            getModifyTopNInfo(),
            getMultiWriteInfo());
        logicalModify.originalSqlNode = originalSqlNode;
        logicalModify.evalRowColumnMetas = evalRowColumnMetas;
        logicalModify.inputToEvalFieldMappings = inputToEvalFieldMappings;
        logicalModify.genColRexNodes = genColRexNodes;
        logicalModify.needCompareWriters = needCompareWriters;
        logicalModify.setColumnTargetMappings = setColumnTargetMappings;
        logicalModify.setColumnSourceMappings = setColumnSourceMappings;
        logicalModify.setColumnMetas = setColumnMetas;
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

    public Map<Integer, List<ColumnMeta>> getEvalRowColumnMetas() {
        return evalRowColumnMetas;
    }

    public void setEvalRowColumnMetas(
        Map<Integer, List<ColumnMeta>> evalRowColumnMetas) {
        this.evalRowColumnMetas = evalRowColumnMetas;
    }

    public Map<Integer, List<Integer>> getInputToEvalFieldMappings() {
        return inputToEvalFieldMappings;
    }

    public void setInputToEvalFieldMappings(
        Map<Integer, List<Integer>> inputToEvalFieldMappings) {
        this.inputToEvalFieldMappings = inputToEvalFieldMappings;
    }

    public Map<Integer, List<RexNode>> getGenColRexNodes() {
        return genColRexNodes;
    }

    public void setGenColRexNodes(
        Map<Integer, List<RexNode>> genColRexNodes) {
        this.genColRexNodes = genColRexNodes;
    }

    public void setNeedCompareWriters(
        Map<DistinctWriter, Integer> needCompareWriters) {
        this.needCompareWriters = needCompareWriters;
    }

    public void setSetColumnTargetMappings(
        Map<Integer, Mapping> setColumnTargetMappings) {
        this.setColumnTargetMappings = setColumnTargetMappings;
    }

    public void setSetColumnSourceMappings(
        Map<Integer, Mapping> setColumnSourceMappings) {
        this.setColumnSourceMappings = setColumnSourceMappings;
    }

    public void setSetColumnMetas(
        Map<Integer, List<ColumnMeta>> setColumnMetas) {
        this.setColumnMetas = setColumnMetas;
    }

    public Map<DistinctWriter, Integer> getNeedCompareWriters() {
        return needCompareWriters;
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

    public void setModifyForeignKey(boolean modifyForeignKey) {
        this.modifyForeignKey = modifyForeignKey;
    }

    public boolean isModifyForeignKey() {
        return this.modifyForeignKey;
    }

    public boolean isModifyTopN() {
        return null != this.modifyTopNInfo && ModifyTopNInfo.EMPTY != this.modifyTopNInfo;
    }

    public boolean isMultiWriteCanBeOptimizedByReturning() {
        return null != this.multiWriteInfo
            && !LogicalMultiWriteInfo.EMPTY.equals(this.multiWriteInfo);
    }

    @Data
    @RequiredArgsConstructor
    public static class ModifyTopNInfo {
        public static ModifyTopNInfo EMPTY = new ModifyTopNInfo();

        private final ImmutableList<String> pkColumnNames;
        private final ImmutableList<RexNode> fieldExps;
        private final RelCollation collation;
        private final RexDynamicParam fetch;

        @Accessors(chain = true)
        private boolean optimizeByReturning = false;

        private ModifyTopNInfo() {
            this.pkColumnNames = ImmutableList.of();
            this.fieldExps = ImmutableList.of();
            this.collation = null;
            this.fetch = null;
        }

        public boolean isDesc() {
            return getCollation().getFieldCollations().get(0).direction.isDescending();
        }

        public static ModifyTopNInfo create(@NotNull List<String> pkColumnarNames,
                                            @NotNull List<RexNode> fieldExps,
                                            @NotNull RelCollation collation,
                                            @NotNull RexDynamicParam fetch) {
            Preconditions.checkArgument(fieldExps.size() == collation.getFieldCollations().size());
            return new ModifyTopNInfo(ImmutableList.copyOf(pkColumnarNames), ImmutableList.copyOf(fieldExps), collation,
                fetch);
        }
    }

    @Data
    @RequiredArgsConstructor
    public static class LogicalMultiWriteInfo {
        public static LogicalMultiWriteInfo EMPTY = new LogicalMultiWriteInfo();

        private final boolean withOffset;
        private final boolean withFetch;

        @Accessors(chain = true)
        private LogicalModifyViewBuilder lmvBuilder = null;

        @Accessors(chain = true)
        private boolean optimizeByReturning = false;

        private LogicalMultiWriteInfo() {
            this.withOffset = false;
            this.withFetch = false;
        }

        public static LogicalMultiWriteInfo create(boolean withOffset, boolean withFetch) {
            return new LogicalMultiWriteInfo(withOffset, withFetch);
        }
    }
}
