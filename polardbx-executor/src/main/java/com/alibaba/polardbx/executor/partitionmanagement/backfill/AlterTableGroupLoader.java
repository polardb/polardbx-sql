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

package com.alibaba.polardbx.executor.partitionmanagement.backfill;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.InsertIndexExecutor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupLoader extends Loader {
    private final Map<String, Pair<String, String>> physicalTableGroupMap;

    protected AlterTableGroupLoader(String schemaName, String tableName, SqlInsert insert, SqlInsert insertIgnore,
                                    ExecutionPlan checkerPlan,
                                    int[] checkerPkMapping,
                                    int[] checkerParamMapping,
                                    BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                    Map<String, Pair<String, String>> physicalTableGroupMap,
                                    boolean mirrorCopy) {
        super(schemaName, tableName, insert, insertIgnore, checkerPlan, checkerPkMapping, checkerParamMapping,
            executeFunc, mirrorCopy);
        this.physicalTableGroupMap = physicalTableGroupMap;
    }

    public static Loader create(String schemaName, String primaryTable, String indexTable,
                                BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                boolean useHint, ExecutionContext ec,
                                Map<String, Pair<String, String>> physicalTableGroupMap, boolean mirrorCopy) {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        // Construct target table
        final SqlNode targetTableParam = BuildPlanUtils.buildTargetTable();

        // Construct targetColumnList
        final TableMeta indexTableMeta = ec.getSchemaManager(schemaName).getTable(indexTable);
        final SqlNodeList targetColumnList = new SqlNodeList(
            indexTableMeta.getAllColumns()
                .stream()
                .filter(columnMeta -> !columnMeta.isGeneratedColumn())
                .map(columnMeta -> new SqlIdentifier(columnMeta.getName(), SqlParserPos.ZERO))
                .collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // Construct values
        final SqlNode[] dynamics = new SqlNode[targetColumnList.size()];
        for (int i = 0; i < targetColumnList.size(); i++) {
            dynamics[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);
        }
        final SqlNode row = new SqlBasicCall(SqlStdOperatorTable.ROW, dynamics, SqlParserPos.ZERO);
        final SqlNode[] rowList = new SqlNode[] {row};
        final SqlNode values = new SqlBasicCall(SqlStdOperatorTable.VALUES, rowList, SqlParserPos.ZERO);

        // Insert
        final SqlInsert sqlInsert = new SqlInsert(SqlParserPos.ZERO,
            new SqlNodeList(SqlParserPos.ZERO),
            targetTableParam,
            values,
            targetColumnList,
            SqlNodeList.EMPTY,
            0,
            null);

        // Construct keyword
        final SqlNodeList keywords = new SqlNodeList(ImmutableList.of(SqlDmlKeyword.IGNORE.symbol(SqlParserPos.ZERO)),
            SqlParserPos.ZERO);

        // Insert ignore
        final SqlInsert sqlInsertIgnore = new SqlInsert(SqlParserPos.ZERO,
            keywords,
            targetTableParam,
            values,
            targetColumnList,
            SqlNodeList.EMPTY,
            0,
            null);

        // For duplicate check
        final TableMeta primaryTableMeta = ec.getSchemaManager(schemaName).getTable(primaryTable);
        final TddlRuleManager tddlRuleManager = optimizerContext.getRuleManager();
        final Set<String> filterColumns = Sets.newTreeSet(String::compareToIgnoreCase);
        final Set<String> primaryKeys = Sets.newTreeSet(String::compareToIgnoreCase);
        primaryKeys.addAll(GlobalIndexMeta.getPrimaryKeys(primaryTableMeta));
        filterColumns.addAll(primaryKeys);
        filterColumns.addAll(tddlRuleManager.getSharedColumns(primaryTable));
        filterColumns.addAll(tddlRuleManager.getSharedColumns(indexTable));

        final List<String> filterList = ImmutableList.copyOf(filterColumns);

        // Mapping from index of filter param to index of insert param
        final int[] checkerParamMapping = new int[filterList.size()];
        final int[] checkerPkMapping = new int[primaryKeys.size()];
        int pkIndex = 0;
        for (Ord<String> ordFilter : Ord.zip(filterList)) {
            for (Ord<SqlNode> ordColumn : Ord.zip(targetColumnList)) {
                final String columnName = ((SqlIdentifier) ordColumn.getValue()).getSimple();
                if (ordFilter.getValue().equalsIgnoreCase(columnName)) {
                    checkerParamMapping[ordFilter.i] = ordColumn.i;
                    if (primaryKeys.contains(columnName)) {
                        checkerPkMapping[pkIndex++] = ordColumn.i;
                    }
                }
            }
        }

        // Build select plan for duplication check
        final ExecutionPlan checkerPlan = PhysicalPlanBuilder
            .buildPlanForBackfillDuplicateCheck(schemaName, indexTableMeta, filterList, filterList, useHint, ec);

        return new AlterTableGroupLoader(schemaName,
            indexTable,
            sqlInsert,
            sqlInsertIgnore,
            checkerPlan,
            checkerPkMapping,
            checkerParamMapping,
            executeFunc,
            physicalTableGroupMap,
            mirrorCopy);
    }

    @Override
    public int executeInsert(SqlInsert sqlInsert, String schemaName, String tableName,
                             ExecutionContext executionContext, String sourceDbIndex, String phyTableName) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        PartitionInfo newPartInfo = tableMeta.getNewPartitionInfo();
        String targetGroup = "";
        if (StringUtils.isNotEmpty(phyTableName) && this.mirrorCopy) {
            Pair<String, String> groupPair = physicalTableGroupMap.get(phyTableName);
            if (groupPair != null && groupPair.getKey().equalsIgnoreCase(sourceDbIndex)) {
                targetGroup = groupPair.getValue();
            }
        }
        return InsertIndexExecutor
            .backfillIntoPartitionedTable(null, sqlInsert, tableMeta, schemaName, executionContext, executeFunc, false,
                newPartInfo, targetGroup, phyTableName, this.mirrorCopy);
    }
}
