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

package com.alibaba.polardbx.executor.gsi.backfill;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.InsertIndexExecutor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class CdasLoader extends Loader {
    private final int[] insertedParamMapping;
    boolean needParamProjection;

    private CdasLoader(String schemaName, String tableName, SqlInsert insert, SqlInsert insertIgnore,
                       ExecutionPlan checkerPlan, int[] checkerPkMapping, int[] checkerParamMapping,
                       BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                       int[] insertedParamMapping, boolean needParamProject, String backfillReturning) {
        super(schemaName, tableName, insert, insertIgnore, checkerPlan, checkerPkMapping, checkerParamMapping,
            executeFunc, false, backfillReturning);
        this.insertedParamMapping = insertedParamMapping;
        this.needParamProjection = needParamProject;
    }

    public static CdasLoader create(String srcSchemaName, String dstSchemaName, String srcTableName,
                                    String dstTableName,
                                    BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                    boolean useHint, ExecutionContext ec, boolean needParamProject) {
        final OptimizerContext optimizerContextDst = OptimizerContext.getContext(dstSchemaName);
        final OptimizerContext optimizerContextSrc = OptimizerContext.getContext(srcSchemaName);

        boolean canUseReturning =
            canUseBackfillReturning(ec, dstSchemaName) && canUseBackfillReturning(ec, srcSchemaName);

        // Construct target table
        final SqlNode targetTableParam = BuildPlanUtils.buildTargetTable();
        // Construct targetColumnList
        //final TableMeta indexTableMeta = ec.getSchemaManager(schemaName).getTable(indexTable);
        final TableMeta dstTableMeta = optimizerContextDst.getLatestSchemaManager().getTable(dstTableName);
        final SqlNodeList targetColumnList = new SqlNodeList(
            dstTableMeta.getAllColumns()
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

        //build inserted data column mapping
        final TableMeta srcTableMeta = optimizerContextDst.getLatestSchemaManager().getTable(srcTableName);
        final List<String> srcTableColumns = srcTableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());
        final List<String> dstTableColumns = dstTableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());
        int[] insertMappings = new int[dstTableColumns.size()];
        Map<String, Integer> srcColumnIdx = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < srcTableColumns.size(); i++) {
            srcColumnIdx.put(srcTableColumns.get(i), i);
        }
        for (int i = 0; i < dstTableColumns.size(); i++) {
            insertMappings[i] = srcColumnIdx.get(dstTableColumns.get(i));
        }

        // For duplicate check
        final TddlRuleManager tddlRuleManagerSrc = optimizerContextSrc.getRuleManager();
        final TddlRuleManager tddlRuleManagerDst = optimizerContextDst.getRuleManager();
        final Set<String> filterColumns = Sets.newTreeSet(String::compareToIgnoreCase);
        final Set<String> primaryKeys = Sets.newTreeSet(String::compareToIgnoreCase);
        final List<String> pkList = GlobalIndexMeta.getPrimaryKeys(srcTableMeta);
        primaryKeys.addAll(pkList);
        filterColumns.addAll(primaryKeys);
        filterColumns.addAll(tddlRuleManagerSrc.getSharedColumns(srcTableName));
        filterColumns.addAll(tddlRuleManagerDst.getSharedColumns(dstTableName));

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
            .buildPlanForBackfillDuplicateCheck(dstSchemaName, dstTableMeta, filterList, filterList, useHint, ec);

        return new CdasLoader(dstSchemaName, dstTableName, sqlInsert, sqlInsertIgnore, checkerPlan, checkerPkMapping,
            checkerParamMapping,
            executeFunc, insertMappings, needParamProject,
            canUseReturning ? String.join(",", pkList) : null);

    }

    @Override
    public int executeInsert(SqlInsert sqlInsert, String schemaName, String tableName,
                             ExecutionContext executionContext, String sourceDbIndex, String phyTableName) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        List<Map<Integer, ParameterContext>> returningRes = new ArrayList<>();
        int affectRows = InsertIndexExecutor
            .insertIntoTable(null, sqlInsert, tableMeta, "", "",
                schemaName, executionContext, executeFunc, false, true,
                "", usingBackfillReturning, returningRes);

        return usingBackfillReturning ? getReturningAffectRows(returningRes, executionContext) : affectRows;
    }

    @Override
    public int fillIntoIndex(List<Map<Integer, ParameterContext>> batchParams,
                             Pair<ExecutionContext, Pair<String, String>> baseEcAndIndexPair,
                             Supplier<Boolean> checker) {
        if (batchParams.isEmpty()) {
            return 0;
        }

        List<Map<Integer, ParameterContext>> batch;
        if (this.needParamProjection) {
            batch = projectBatchParam(batchParams);
        } else {
            batch = batchParams;
        }

        return super.fillIntoIndex(batch, baseEcAndIndexPair, checker);
    }

    protected List<Map<Integer, ParameterContext>> projectBatchParam(List<Map<Integer, ParameterContext>> batchParams) {
        final List<Map<Integer, ParameterContext>> newBatchParam = new ArrayList<>();
        for (int i = 0; i < batchParams.size(); i++) {
            final Map<Integer, ParameterContext> param = new HashMap<>();
            for (int j = 0; j < this.insertedParamMapping.length; j++) {
                final int oldIdx = this.insertedParamMapping[j] + 1;
                final ParameterContext old = batchParams.get(i).get(oldIdx);
                param.put(j + 1,
                    new ParameterContext(old.getParameterMethod(), new Object[] {j + 1, old.getArgs()[1]}));
            }
            newBatchParam.add(param);
        }
        return newBatchParam;
    }

}
