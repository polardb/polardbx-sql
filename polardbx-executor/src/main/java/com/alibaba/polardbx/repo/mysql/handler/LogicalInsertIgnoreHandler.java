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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsertIgnore;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author chenmo.cm
 */
public class LogicalInsertIgnoreHandler extends LogicalInsertHandler {
    public LogicalInsertIgnoreHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected int doExecute(LogicalInsert insert, ExecutionContext executionContext,
                            LogicalInsert.HandlerParams handlerParams) {
        // Need auto-savepoint only when auto-commit = 0.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());

        final LogicalInsertIgnore insertIgnore = (LogicalInsertIgnore) insert;
        final String schemaName = insertIgnore.getSchemaName();
        final String tableName = insertIgnore.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        int affectRows = 0;
        // For batch insertIgnore, change params index.
        if (insertIgnore.getBatchSize() > 0) {
            insertIgnore.buildParamsForBatch(executionContext);
        }

        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, gsiConcurrentWrite);
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        // INSERT IGNORE with NODE/SCAN hint specified
        if (insertIgnore.hasHint()) {
            final List<RelNode> inputs = replaceSeqAndBuildPhyPlan(insertIgnore, executionContext, handlerParams);

            return executePhysicalPlan(inputs, executionContext, schemaName, isBroadcast);
        }

        // Append parameter for RexCallParam and RexSequenceParam
        RexUtils.updateParam(insertIgnore, executionContext, false, handlerParams);

        // Build ExecutionContext for insert
        final ExecutionContext insertEc = executionContext.copy();

        // Try to exec by pushDown policy for scale-out
        Integer execAffectRow = tryPushDownExecute(insertIgnore, schemaName, tableName, insertEc);
        if (execAffectRow != null) {
            return execAffectRow;
        }

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        final RelDataType selectRowType = getRowTypeForDuplicateCheck(insertIgnore);

        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        final boolean allDnUseXDataSource = isAllDnUseXDataSource(topologyHandler);
        final boolean gsiCanUseReturning =
            isGsiCanUseReturning(insertIgnore.getTargetTables().get(0), executionContext);
        final boolean isColumnMultiWriting =
            TableColumnUtils.isModifying(schemaName, tableName, executionContext);
        final boolean checkPrimaryKey =
            executionContext.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK);
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks() && tableMeta.hasForeignKey();

        // Disable returning when doing column multi-writing since we have not tested it yet
        boolean canUseReturning =
            executorContext.getStorageInfoManager().supportsReturning() && executionContext.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_RETURNING) && allDnUseXDataSource && gsiCanUseReturning
                && !isBroadcast && !ComplexTaskPlanUtils.canWrite(tableMeta) && !isColumnMultiWriting
                && !checkPrimaryKey && !checkForeignKey;

        if (canUseReturning) {
            canUseReturning = noDuplicateOrNullValues(insertIgnore, insertEc);
        }

        if (canUseReturning) {
            // Optimize by insert ignore returning

            final List<RelNode> allPhyPlan =
                new ArrayList<>(replaceSeqAndBuildPhyPlan(insertIgnore, insertEc, handlerParams));
            getPhysicalPlanForGsi(insertIgnore.getGsiInsertIgnoreWriters(), insertEc, allPhyPlan);

            // Mix execute
            final Map<String, List<List<Object>>> tableInsertedValues =
                executeAndGetReturning(executionContext, allPhyPlan, insertIgnore, insertEc, memoryAllocator,
                    selectRowType);

            int affectedRows =
                Optional.ofNullable(tableInsertedValues.get(insertIgnore.getLogicalTableName())).map(List::size)
                    .orElse(0);

            final boolean returnIgnored =
                executionContext.getParamManager().getBoolean(ConnectionParams.DML_RETURN_IGNORED_COUNT);
            int ignoredRows = 0;
            int totalRows = 0;
            if (returnIgnored) {
                final LogicalDynamicValues input = RelUtils.getRelInput(insertIgnore);
                final Parameters params = executionContext.getParams();
                final int batchSize = params.isBatch() ? params.getBatchSize() : 1;
                totalRows = batchSize * input.getTuples().size();
                ignoredRows = totalRows - affectedRows;
            }

            // Generate delete
            final List<String> targetTableNames = new ArrayList<>();
            targetTableNames.add(tableName);
            targetTableNames.addAll(insertIgnore.getGsiInsertIgnoreWriters().stream()
                .map(writer -> writer.getInsert().getLogicalTableName()).collect(Collectors.toList()));

            // If any of insert ignore returning executed above returns nothing, means that we should remove all rows inserted.
            // This could be accomplished by rollback to savepoint before this logical statement.
            // We should optimize this code after auto savepoint supported.
            final boolean removeAllInserted =
                targetTableNames.stream().anyMatch(tn -> !tableInsertedValues.containsKey(tn));

            if (removeAllInserted) {
                // Remove all inserted
                affectedRows -=
                    removeInserted(insertIgnore, schemaName, tableName, isBroadcast, insertEc, tableInsertedValues);
                if (returnIgnored) {
                    ignoredRows = totalRows;
                }
            } else {
                // Remove part of inserted
                final List<Integer> beforePkMapping = insertIgnore.getBeforePkMapping();
                final List<ColumnMeta> pkColumnMetas = insertIgnore.getPkColumnMetas();

                final Map<String, List<List<Object>>> tableDeletePks =
                    getRowsToBeRemoved(tableName, tableInsertedValues, beforePkMapping, pkColumnMetas);

                affectedRows -=
                    removeInserted(insertIgnore, schemaName, tableName, isBroadcast, insertEc, tableDeletePks);
                if (returnIgnored) {
                    ignoredRows +=
                        Optional.ofNullable(tableDeletePks.get(insertIgnore.getLogicalTableName())).map(List::size)
                            .orElse(0);
                }

            }

            handlerParams.optimizedWithReturning = true;
            // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
            executionContext.setPhySqlId(insertEc.getPhySqlId() + 1);
            if (returnIgnored) {
                return ignoredRows;
            } else {
                return affectedRows;
            }
        } else {
            handlerParams.optimizedWithReturning = false;
        }

        try {
            if (insertIgnore.isUkContainGeneratedColumn()) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "INSERT IGNORE on table having VIRTUAL/STORED generated column in unique key");
            }

            Map<String, List<List<String>>> ukGroupByTable = insertIgnore.getUkGroupByTable();
            Map<String, List<String>> localIndexPhyName = insertIgnore.getLocalIndexPhyName();
            List<Map<Integer, ParameterContext>> deduplicated;
            List<List<Object>> duplicateValues;
            List<List<Object>> convertedValues = new ArrayList<>();
            boolean usePartFieldChecker = insertIgnore.isUsePartFieldChecker() && executionContext.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_NEW_DUP_CHECKER);

            // Select and lock duplicated values on primary table and GSI
            duplicateValues = getDuplicatedValues(insertIgnore, LockMode.SHARED_LOCK, executionContext, ukGroupByTable,
                localIndexPhyName, (rowCount) -> memoryAllocator.allocateReservedMemory(
                    MemoryEstimator.calcSelectValuesMemCost(rowCount, selectRowType)), selectRowType, true,
                handlerParams, convertedValues, usePartFieldChecker);

            // Get deduplicated batch parameters
            final List<Map<Integer, ParameterContext>> batchParameters =
                executionContext.getParams().getBatchParameters();

            // Duplicate might exists between insert values
            // We should deduplicate in one pass, otherwise may return different result compared to MySQL
            deduplicated = usePartFieldChecker ?
                getDeduplicatedParamsWithNewGroupKey(insertIgnore.getUkColumnMetas(), insertIgnore.getBeforeUkMapping(),
                    insertIgnore.getAfterUkMapping(), duplicateValues, convertedValues, batchParameters,
                    executionContext) :
                getDeduplicatedParams(insertIgnore.getUkColumnMetas(), insertIgnore.getBeforeUkMapping(),
                    insertIgnore.getAfterUkMapping(), RelUtils.getRelInput(insertIgnore), duplicateValues,
                    batchParameters, executionContext);

            if (!deduplicated.isEmpty()) {
                insertEc.setParams(new Parameters(deduplicated));
                if (checkForeignKey) {
                    deduplicated = beforeInsertCheck(insertIgnore, deduplicated, insertEc);
                    if (deduplicated.isEmpty()) {
                        return affectRows;
                    }
                    insertEc.setParams(new Parameters(deduplicated));
                }
            } else {
                // All duplicated
                return affectRows;
            }

            try {
                if (gsiConcurrentWrite) {
                    affectRows = concurrentExecute(insertIgnore, insertEc);
                } else {
                    affectRows = sequentialExecute(insertIgnore, insertEc);
                }
            } catch (Throwable e) {
                handleException(executionContext, e, GeneralUtil.isNotEmpty(insertIgnore.getGsiInsertWriters()));
            }
        } finally {
            selectValuesPool.destroy();
        }
        // Insert batch may be split in TConnection, so we need to set executionContext's PhySqlId for next part
        executionContext.setPhySqlId(insertEc.getPhySqlId() + 1);
        return affectRows;
    }

    protected List<Map<Integer, ParameterContext>> beforeInsertCheck(LogicalInsert logicalInsert,
                                                                     List<Map<Integer, ParameterContext>> deduplicated,
                                                                     ExecutionContext executionContext) {
        LogicalDynamicValues input = RelUtils.getRelInput(logicalInsert);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        List<String> insertColumns = input.getRowType().getFieldNames().stream().map(String::toUpperCase).collect(
            Collectors.toList());
        List<List<Object>> values = deduplicated.stream().map(
            param -> {
                List<Object> value = new ArrayList<>();
                for (int i = 0; i < insertColumns.size(); i++) {
                    value.add(RexUtils.getValueFromRexNode(rexRow.get(i), executionContext, param));
                }
                return value;
            }
        ).collect(Collectors.toList());

        List<Map<Integer, ParameterContext>> result = new ArrayList<>();
        result = beforeInsertFkCheckIgnore(logicalInsert, logicalInsert.getLogicalTableName(), executionContext, values,
            deduplicated);

        return result;
    }

    /**
     * Check whether all dn using XProtocol
     */
    public boolean isAllDnUseXDataSource(TopologyHandler topologyHandler) {
        return topologyHandler.getGroupNames().stream()
            .allMatch(groupName -> Optional.ofNullable(topologyHandler.get(groupName))
                .map((Function<IGroupExecutor, Object>) IGroupExecutor::getDataSource)
                .map(ds -> ds instanceof TGroupDataSource && ((TGroupDataSource) ds).isXDataSource()).orElse(false));
    }

    /**
     * intersect inserted pk set of primary and gsi table and get pk set of rows should not inserted
     */
    private Map<String, List<List<Object>>> getRowsToBeRemoved(String tableName,
                                                               Map<String, List<List<Object>>> tableInsertedValues,
                                                               List<Integer> beforePkMapping,
                                                               List<ColumnMeta> pkColumnMetas) {
        final Map<String, Set<GroupKey>> tableInsertedPks = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, List<Pair<GroupKey, List<Object>>>> tablePkRows =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableInsertedValues.forEach((tn, insertedValues) -> {
            final Set<GroupKey> insertedPks = new TreeSet<>();
            final List<Pair<GroupKey, List<Object>>> pkRows = new ArrayList<>();
            for (List<Object> inserted : insertedValues) {
                final Object[] groupKeys = beforePkMapping.stream().map(inserted::get).toArray();
                final GroupKey pk = new GroupKey(groupKeys, pkColumnMetas);
                insertedPks.add(pk);
                pkRows.add(Pair.of(pk, inserted));
            }
            tableInsertedPks.put(tn, insertedPks);
            tablePkRows.put(tn, pkRows);
        });

        // Get intersect of inserted values
        final Set<GroupKey> distinctPks = new TreeSet<>();
        for (GroupKey pk : tableInsertedPks.get(tableName)) {
            if (tableInsertedPks.values().stream().allMatch(pks -> pks.contains(pk))) {
                distinctPks.add(pk);
            }
        }

        // Remove values which not exists in at least one insert results
        final Map<String, List<List<Object>>> tableDeletePks = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tablePkRows.forEach((tn, pkRows) -> {
            final List<List<Object>> deletePks = new ArrayList<>();
            pkRows.forEach(pkRow -> {
                if (!distinctPks.contains(pkRow.getKey())) {
                    deletePks.add(pkRow.getValue());
                }
            });
            if (!deletePks.isEmpty()) {
                tableDeletePks.put(tn, deletePks);
            }
        });
        return tableDeletePks;
    }

    /**
     * remove inserted rows
     */
    private int removeInserted(LogicalInsertIgnore insertIgnore, String schemaName, String primaryTableName,
                               boolean isBroadcast, ExecutionContext insertEc,
                               Map<String, List<List<Object>>> tableInsertedValues) {
        int removedRows = 0;
        final List<RelNode> allPhyDelete = new ArrayList<>();

        // Generate delete for primary
        final List<List<Object>> primaryInserted = tableInsertedValues.get(primaryTableName);
        if (null != primaryInserted) {
            final List<List<Object>> primaryDeleteRows =
                buildDeleteParams(insertIgnore, insertEc, primaryInserted);

            allPhyDelete
                .addAll(insertIgnore.getPrimaryDeleteWriter().getInput(insertEc, (w) -> primaryDeleteRows));

            removedRows = primaryDeleteRows.size();
        }

        // Generate delete for gsi
        insertIgnore.getGsiDeleteWriters().forEach(
            gsiDeleteWriter -> {
                final String gsiTable = RelUtils.getQualifiedTableName(gsiDeleteWriter.getTargetTable()).right;

                final List<List<Object>> gsiInserted = tableInsertedValues.get(gsiTable);
                if (null != gsiInserted) {
                    final List<List<Object>> gsiDeleteRows =
                        buildDeleteParams(insertIgnore, insertEc, gsiInserted);
                    allPhyDelete.addAll(gsiDeleteWriter.getInput(insertEc, (w) -> gsiDeleteRows));
                }
            });

        executePhysicalPlan(allPhyDelete, insertEc, schemaName, isBroadcast);
        return removedRows;
    }

    private List<List<Object>> buildDeleteParams(LogicalInsertIgnore insertIgnore, ExecutionContext ec,
                                                 List<List<Object>> inserted) {
        final List<List<Object>> deleteRows = new ArrayList<>();

        LogicalDynamicValues input = RelUtils.getRelInput(insertIgnore);
        final List<Map<Integer, ParameterContext>> params = buildDistinctParams(ec, insertIgnore, inserted);

        if (null == params) {
            return deleteRows;
        }

        params.forEach(paramRow -> input.getTuples().forEach(tuple -> deleteRows.add(
            tuple.stream().map(rex -> RexUtils.getValueFromRexNode(rex, ec, paramRow))
                .collect(Collectors.toList()))));
        return deleteRows;
    }

    private boolean noDuplicateOrNullValues(LogicalInsertIgnore insertIgnore, ExecutionContext insertEc) {
        final List<List<ColumnMeta>> ukColumnMetas = insertIgnore.getUkColumnMetas();
        final List<List<Integer>> ukColumnsListAfterUkMapping = insertIgnore.getAfterUkMapping();
        final List<Integer> ugsiUkIndex = insertIgnore.getAfterUgsiUkIndex();

        final LogicalDynamicValues input = RelUtils.getRelInput(insertIgnore);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final List<Map<Integer, ParameterContext>> parameters = insertEc.getParams().getBatchParameters();
        final List<Set<GroupKey>> checkers = new ArrayList<>(ukColumnMetas.size());
        IntStream.range(0, ukColumnMetas.size()).forEach(i -> checkers.add(new TreeSet<>()));

        for (Map<Integer, ParameterContext> paramRow : parameters) {
            final List<GroupKey> insertRow = ExecUtils
                .buildGroupKeys(ukColumnsListAfterUkMapping, ukColumnMetas,
                    (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), insertEc, paramRow));

            final boolean duplicateExists =
                IntStream.range(0, ukColumnMetas.size()).anyMatch(i -> {
                    if (checkers.get(i).contains(insertRow.get(i))) {
                        return true;
                    } else {
                        checkers.get(i).add(insertRow.get(i));
                        return false;
                    }
                });

            final boolean nullExists = ugsiUkIndex.stream()
                .anyMatch(i -> Arrays.stream(insertRow.get(i).getGroupKeys()).anyMatch(Objects::isNull));

            if (duplicateExists || nullExists) {
                return false;
            }
        }
        return true;
    }

    private List<Map<Integer, ParameterContext>> buildDistinctParams(ExecutionContext executionContext,
                                                                     LogicalInsertIgnore insertIgnore,
                                                                     List<List<Object>> distinctValues) {
        final List<Integer> beforePkMapping = insertIgnore.getBeforePkMapping();
        final List<Integer> afterPkMapping = insertIgnore.getAfterPkMapping();
        final List<ColumnMeta> pkColumnMetas = insertIgnore.getPkColumnMetas();

        // Build duplicate checker
        final List<GroupKey> checkers = new ArrayList<>();
        for (List<Object> distinctValue : distinctValues) {
            final Object[] groupKeys = beforePkMapping.stream().map(distinctValue::get).toArray();
            checkers.add(new GroupKey(groupKeys, pkColumnMetas));
        }

        if (checkers.isEmpty()) {
            return null;
        }

        // Deduplicate batch parameters
        final LogicalDynamicValues dynamicValues = RelUtils.getRelInput(insertIgnore);
        final ImmutableList<RexNode> rexRow = dynamicValues.getTuples().get(0);

        // Get deduplicated batch parameters
        final List<Map<Integer, ParameterContext>> currentBatchParameters =
            executionContext.getParams().getBatchParameters();

        // Ignore last
        return currentBatchParameters.stream().filter(row -> {
            final Object[] groupKeys =
                afterPkMapping.stream().map(i -> RexUtils.getValueFromRexNode(rexRow.get(i), executionContext, row))
                    .toArray();
            final GroupKey insertRow = new GroupKey(groupKeys, pkColumnMetas);

            if (checkers.isEmpty()) {
                return false;
            }

            final int checkerNum = IntStream.range(0, checkers.size()).boxed()
                .filter(i -> ExecUtils.duplicated(checkers.get(i), insertRow))
                .findFirst().orElse(-1);

            if (checkerNum >= 0) {
                checkers.remove(checkerNum);
            }

            return checkerNum >= 0;
        }).collect(Collectors.toList());
    }

    protected Map<String, List<List<Object>>> executeAndGetReturning(ExecutionContext executionContext,
                                                                     List<RelNode> inputs,
                                                                     LogicalInsertIgnore insertIgnore,
                                                                     ExecutionContext insertEc,
                                                                     MemoryAllocatorCtx memoryAllocator,
                                                                     RelDataType rowType) {
        final String schemaName = insertIgnore.getSchemaName();
        final String tableName = insertIgnore.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = null != or && or.isBroadCast(tableName);

        final String currentReturning = insertEc.getReturning();
        insertEc.setReturning(String.join(",", insertIgnore.getPkColumnNames()));

        try {
            QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(executionContext);
            // If there's a broadcast table, the concurrency will be set to
            // FIRST_THEN. But when modifying multi tb, the concurrency can't be
            // FIRST_THEN, which causes concurrent transaction error.
            if (!isBroadcast && queryConcurrencyPolicy == QueryConcurrencyPolicy.FIRST_THEN_CONCURRENT) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
                if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM)) {
                    queryConcurrencyPolicy = QueryConcurrencyPolicy.RELAXED_GROUP_CONCURRENT;
                }
            }
            if (inputs.size() == 1) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;
            }

            final List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(insertEc, inputs, queryConcurrencyPolicy, inputCursors, schemaName);

            final Map<String, List<List<Object>>> tableDistinctValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Cursor cursor : inputCursors) {
                if (cursor instanceof GroupConcurrentUnionCursor) {
                    final GroupConcurrentUnionCursor groupConcurrentUnionCursor = (GroupConcurrentUnionCursor) cursor;

                    int currentCursorIndex = -1;
                    Cursor currentCursor = null;
                    int rowCount = 0;
                    List<List<Object>> distinctValues = new ArrayList<>();
                    Row rs = null;
                    while ((rs = cursor.next()) != null) {
                        // Allocator memory
                        if ((++rowCount) % TddlConstants.DML_SELECT_BATCH_SIZE_DEFAULT == 0) {
                            memoryAllocator
                                .allocateReservedMemory(MemoryEstimator.calcSelectValuesMemCost(rowCount, rowType));
                            rowCount = 0;
                        }

                        if (currentCursorIndex != groupConcurrentUnionCursor.getCurrentIndex()) {
                            // switch cursor
                            currentCursorIndex = groupConcurrentUnionCursor.getCurrentIndex();
                            currentCursor = groupConcurrentUnionCursor.getCurrentCursor();
                            final MyPhyTableModifyCursor modifyCursor = (MyPhyTableModifyCursor) currentCursor;
                            final String logicalTableName =
                                ((LogicalInsert) modifyCursor.getPlan().getParent()).getLogicalTableName();
                            distinctValues =
                                tableDistinctValues.computeIfAbsent(logicalTableName, (k) -> new ArrayList<>());
                        }

                        final List<Object> rawValues = rs.getValues();
                        final List<Object> outValues = new ArrayList<>(rawValues.size());
                        final List<ColumnMeta> columnMetas = cursor.getReturnColumns();
                        for (int i = 0; i < rawValues.size(); i++) {
                            outValues.add(DataTypeUtil.toJavaObject(
                                GeneralUtil.isNotEmpty(columnMetas) ? columnMetas.get(i) : null, rawValues.get(i)));
                        }
                        distinctValues.add(outValues);
                    }
                } else if (cursor instanceof MyPhyTableModifyCursor) {
                    final MyPhyTableModifyCursor modifyCursor = (MyPhyTableModifyCursor) cursor;
                    final PhyTableOperation tableOperation = (PhyTableOperation) modifyCursor.getPlan();
                    final String logicalTableName = tableOperation.getLogicalTableNames().get(0);
                    final List<List<Object>> distinctValues =
                        tableDistinctValues.computeIfAbsent(logicalTableName, (k) -> new ArrayList<>());
                    try {
                        final List<List<Object>> rows = getQueryResult(cursor, (rowCount) -> memoryAllocator
                            .allocateReservedMemory(MemoryEstimator.calcSelectValuesMemCost(rowCount, rowType)));

                        distinctValues.addAll(rows);
                    } catch (Exception e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, "error when close result");
                    } finally {
                        cursor.close(new ArrayList<>());
                    }
                } else {
                    // Do not support broadcast now
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "unsupported cursor type " + cursor.getClass().getName());
                }
            }

            // Increase physical sql id
            executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);

            return tableDistinctValues;
        } finally {
            insertEc.setReturning(currentReturning);
        }
    }

    protected RelDataType getRowTypeForDuplicateCheck(LogicalInsertIgnore insertIgnore) {
        final RelNode input = insertIgnore.getInput();
        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(input.getRowType().getFieldNames()).forEach(o -> columnIndexMap.put(o.getValue(), o.getKey()));

        final List<String> selectKeyNames = insertIgnore.getSelectListForDuplicateCheck();
        final List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
        final List<RelDataType> fieldTypes = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        selectKeyNames.forEach(columnName -> {
            final RelDataTypeField relDataTypeField = inputFields.get(columnIndexMap.get(columnName));
            fieldTypes.add(relDataTypeField.getType());
            fieldNames.add(relDataTypeField.getName());
        });
        return insertIgnore.getCluster().getTypeFactory().createStructType(fieldTypes, fieldNames);
    }

    private int concurrentExecute(LogicalInsertIgnore insertIgnore, ExecutionContext insertEc) {
        final String schemaName = insertIgnore.getSchemaName();
        final String tableName = insertIgnore.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        List<RelNode> inputs = insertIgnore.getPrimaryInsertWriter().getInput(insertEc);

        // Get plan for primary
        final List<RelNode> primaryPhyPlan =
            inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());

        final List<RelNode> replicatePhyPlan =
            inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());

        final List<RelNode> allInsertPhyPlan = new ArrayList<>(primaryPhyPlan);
        allInsertPhyPlan.addAll(replicatePhyPlan);
        // Get plan for gsi
        final AtomicInteger gsiInsertWriterCount =
            getPhysicalPlanForGsi(insertIgnore.getGsiInsertWriters(), insertEc, allInsertPhyPlan);

        // Execute
        final int totalInsertAffectRows =
            executePhysicalPlan(allInsertPhyPlan, insertEc, schemaName, isBroadcast);

        boolean multiWriteWithoutBroadcast =
            (gsiInsertWriterCount.get() > 0 || GeneralUtil.isNotEmpty(replicatePhyPlan)) && !isBroadcast;
        boolean multiWriteWithBroadcast =
            (gsiInsertWriterCount.get() > 0 || GeneralUtil.isNotEmpty(replicatePhyPlan)) && isBroadcast;

        if (multiWriteWithoutBroadcast) {
            return primaryPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows())
                .sum();
        } else if (multiWriteWithBroadcast) {
            return ((BaseQueryOperation) primaryPhyPlan.get(0)).getAffectedRows();
        } else {
            return totalInsertAffectRows;
        }
    }

    private AtomicInteger getPhysicalPlanForGsi(List<InsertWriter> gsiInsertWriters,
                                                ExecutionContext insertEc,
                                                List<RelNode> allInsertPhyPlan) {
        final AtomicInteger gsiInsertWriterCount = new AtomicInteger(0);
        gsiInsertWriters.stream()
            .flatMap(gsiInsertWriter -> {
                final List<RelNode> phyPlans = gsiInsertWriter.getInput(insertEc.copy());
                if (phyPlans.size() > 0) {
                    gsiInsertWriterCount.getAndIncrement();
                }
                return phyPlans.stream();
            })
            .forEach(allInsertPhyPlan::add);
        return gsiInsertWriterCount;
    }

    private int sequentialExecute(LogicalInsertIgnore insertIgnore, ExecutionContext insertEc) {
        final String schemaName = insertIgnore.getSchemaName();
        final String tableName = insertIgnore.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);

        List<RelNode> inputs = insertIgnore.getPrimaryInsertWriter().getInput(insertEc);

        // Get plan for primary
        final List<RelNode> primaryInsertPlan =
            inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());

        // Execute for primary
        final int affectedRows = executePhysicalPlan(primaryInsertPlan, insertEc, schemaName, isBroadcast);

        final List<RelNode> replicateInsertPlan =
            inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());

        if (GeneralUtil.isNotEmpty(replicateInsertPlan)) {
            executePhysicalPlan(replicateInsertPlan, insertEc.copy(), schemaName, isBroadcast);
        }
        // Get plan and execute for gsi
        final List<InsertWriter> gsiInsertWriters = insertIgnore.getGsiInsertWriters();
        gsiInsertWriters.forEach(gsiInsertWriter -> {
            final ExecutionContext gsiInsertEc = insertEc.copy();
            List<RelNode> gsiInputs = gsiInsertWriter.getInput(gsiInsertEc);
            final List<RelNode> gsiInsertPlan =
                gsiInputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());
            final List<RelNode> replicateGsiInsertPlan =
                gsiInputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());

            executePhysicalPlan(gsiInsertPlan, gsiInsertEc, schemaName, isBroadcast);
            if (GeneralUtil.isNotEmpty(replicateGsiInsertPlan)) {
                executePhysicalPlan(replicateGsiInsertPlan, gsiInsertEc, schemaName, isBroadcast);
            }

        });

        return affectedRows;
    }

    protected List<RelNode> buildSelects(LogicalInsertIgnore insertIgnore, LockMode lockMode,
                                         ExecutionContext executionContext, String tableName,
                                         List<String> selectColumns, List<String> insertColumns,
                                         List<List<String>> ukColumnsList, List<List<ColumnMeta>> ukColumnMetas,
                                         List<String> ukNameList, int ukIndexOffset, List<Set<String>> currentUkSets,
                                         List<List<Object>> values, boolean withValueIndex,
                                         boolean lookUpPrimaryFromGsi) {
        final String schemaName = insertIgnore.getSchemaName();

        // Get plan for finding duplicate values
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert oc != null;
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        Map<String, String> columnMapping =
            TableColumnUtils.getColumnMultiWriteMapping(tableMeta.getTableColumnMeta());
        if (MapUtils.isNotEmpty(columnMapping)) {
            insertColumns = insertColumns.stream().map(e -> columnMapping.getOrDefault(e.toLowerCase(), e))
                .collect(Collectors.toList());
        }

        // 下面这一大段很复杂的代码-- 非常不宜阅读
        // 大概就是构造一个 value 位置的映射和上层函数中的 uk 位置的映射，以及 pk 的值
        // ukIndexOffset 竟然是调用ta的函数中 uk 数组的位置

        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(insertColumns).forEach(o -> columnIndexMap.put(o.getValue(), o.getKey()));

        final List<List<Integer>> valuesUkMapping = new ArrayList<>(ukColumnsList.size());
        for (final List<String> ukColumns : ukColumnsList) {
            final List<Integer> valueUkColumnIndexes = new ArrayList<>(ukColumns.size());
            for (String ukColumn : ukColumns) {
                valueUkColumnIndexes.add(columnIndexMap.get(ukColumn));
            }
            valuesUkMapping.add(valueUkColumnIndexes);
        }

        // We have converted value before, so GroupKey should be enough
        final List<List<List<Object>>> lookUpUniqueKey = new ArrayList<>();
        final List<List<Pair<Integer, Integer>>> lookUpUniqueKeyIndex = new ArrayList<>();

        IntStream.range(0, ukColumnsList.size()).forEach(i -> lookUpUniqueKey.add(new ArrayList<>()));
        IntStream.range(0, ukColumnsList.size()).forEach(i -> lookUpUniqueKeyIndex.add(new ArrayList<>()));

        if (lookUpPrimaryFromGsi) {
            // We can not dedup if lookUpPrimaryFromGsi is true, otherwise we may miss out some values with same uk but
            // in different shards in primary table
            for (int k = 0; k < values.size(); k++) {
                List<Object> value = values.get(k);
                final List<GroupKey> groupKeys = ExecUtils.buildGroupKeys(valuesUkMapping, ukColumnMetas, value::get);
                for (int i = 0; i < valuesUkMapping.size(); i++) {
                    lookUpUniqueKey.get(i).add(Arrays.asList(groupKeys.get(i).getGroupKeys()));
                    lookUpUniqueKeyIndex.get(i).add(new Pair<>(k, i + ukIndexOffset));
                }
            }
        } else {
            final List<Map<GroupKey, List<Object>>> deduplicated = new ArrayList<>(ukColumnsList.size());
            final List<Map<GroupKey, Pair<Integer, Integer>>> deduplicatedIndex = new ArrayList<>(ukColumnsList.size());

            IntStream.range(0, ukColumnsList.size()).forEach(i -> deduplicated.add(new TreeMap<>()));
            // deduplicatedIndex: (valueIndex, ukIndex)
            IntStream.range(0, ukColumnsList.size()).forEach(i -> deduplicatedIndex.add(new TreeMap<>()));

            for (int k = 0; k < values.size(); k++) {
                List<Object> value = values.get(k);
                final List<GroupKey> groupKeys = ExecUtils.buildGroupKeys(valuesUkMapping, ukColumnMetas, value::get);
                for (int i = 0; i < valuesUkMapping.size(); i++) {
                    deduplicated.get(i).put(groupKeys.get(i), Arrays.asList(groupKeys.get(i).getGroupKeys()));
                    deduplicatedIndex.get(i).put(groupKeys.get(i), new Pair<>(k, i + ukIndexOffset));
                }
            }

            for (int i = 0; i < ukColumnsList.size(); i++) {
                for (Map.Entry<GroupKey, List<Object>> dedup : deduplicated.get(i).entrySet()) {
                    lookUpUniqueKey.get(i).add(dedup.getValue());
                    lookUpUniqueKeyIndex.get(i).add(deduplicatedIndex.get(i).get(dedup.getKey()));
                }
            }
        }

        final boolean singleOrBroadcast =
            Optional.ofNullable(oc.getRuleManager()).map(rule -> !rule.isShard(tableName)).orElse(true);
        final boolean localUkOnly = !tableMeta.withGsiExcludingPureCci();
        final boolean fullTableScanForLocalUk = executionContext.getParamManager()
            .getBoolean(ConnectionParams.DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN);
        final boolean everyUkContainsAllTablePartitionKey =
            GlobalIndexMeta.isEveryUkContainsTablePartitionKey(tableMeta, currentUkSets);
        final boolean checkLocalUkOnly = everyUkContainsAllTablePartitionKey
            || (localUkOnly && !fullTableScanForLocalUk);

        // if lookUpPrimaryFromGsi is true, then there is no need to do fullTableScan since we have selected primary
        // sharding key in previous step
        boolean fullTableScan = singleOrBroadcast || (!checkLocalUkOnly && !lookUpPrimaryFromGsi);

        List<RelNode> selects;
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
        final int maxSqlUnionCount = executionContext.getParamManager().getInt(ConnectionParams.DML_GET_DUP_UNION_SIZE);
        final boolean useIn = executionContext.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_USING_IN);
        final int maxSqlInCount = executionContext.getParamManager().getInt(ConnectionParams.DML_GET_DUP_IN_SIZE);
        // Use IN instead of UNION may cause more deadlocks
        // Ref: https://dev.mysql.com/doc/refman/5.7/en/innodb-locks-set.html
        // If it's select with value index, we can not use IN because each select has its own value index

        selects = withValueIndex || !useIn ?
            builder.buildSelectUnionAndParam(insertIgnore, ukColumnsList, tableMeta, lockMode, values, insertColumns,
                lookUpUniqueKey, lookUpUniqueKeyIndex, selectColumns, withValueIndex, maxSqlUnionCount, fullTableScan) :
            builder.buildSelectInAndParam(insertIgnore, ukColumnsList, ukNameList, tableMeta, lockMode, values,
                insertColumns, lookUpUniqueKey, lookUpUniqueKeyIndex, selectColumns, maxSqlInCount, fullTableScan);

        return selects;
    }

    /**
     * Get duplicate values. Used by INSERT IGNORE / UPSERT / REPLACE.
     */
    protected List<List<Object>> getDuplicatedValues(LogicalInsertIgnore insert, LockMode lockMode,
                                                     ExecutionContext executionContext,
                                                     Map<String, List<List<String>>> ukGroupByTable,
                                                     Map<String, List<String>> localIndexPhyName,
                                                     Consumer<Integer> memoryAllocator, RelDataType selectRowType,
                                                     boolean isInsertIgnore, LogicalInsert.HandlerParams handlerParams,
                                                     List<List<Object>> outConvertedValues,
                                                     boolean usePartFieldChecker) {
        final String schemaName = insert.getSchemaName();
        final String primaryTableName = insert.getLogicalTableName();
        TableMeta primaryTableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
        List<String> primaryKey = GlobalIndexMeta.getPrimaryKeys(primaryTableMeta);
        // PK and primary table's SK
        List<String> primaryAndShardingKey =
            GlobalIndexMeta.getPrimaryAndShardingKeys(primaryTableMeta, Collections.emptyList(), schemaName);
        Map<String, Integer> primaryAndShardingKeyMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < primaryAndShardingKey.size(); i++) {
            primaryAndShardingKeyMap.put(primaryAndShardingKey.get(i), i);
        }
        List<String> autoIncColumns = primaryTableMeta.getAutoIncrementColumns();

        final LogicalDynamicValues input = RelUtils.getRelInput(insert);
        List<List<Object>> values =
            getInputValues(input, executionContext.getParams().getBatchParameters(), executionContext);
        List<String> insertColumns = input.getRowType().getFieldNames().stream().map(String::toUpperCase).collect(
            Collectors.toList());

        // Convert value before select, only for comparison, using original value to insert
        if (usePartFieldChecker) {
            Map<String, ColumnMeta> columnMetaMap = insert.getColumnMetaMap();
            final Parameters params = executionContext.getParams();
            final boolean isBatch = params.isBatch();

            for (int i = 0; i < values.size(); i++) {
                List<Object> value = values.get(i);

                for (int j = 0; j < insertColumns.size(); j++) {
                    ColumnMeta columnMeta = columnMetaMap.get(insertColumns.get(j));
                    if (columnMeta != null) {
                        value.set(j, DataTypeUtil.toJavaObject(columnMeta,
                            RexUtils.convertValue(value.get(j), input.getTuples().get(isBatch ? 0 : i).get(j), false,
                                columnMeta, executionContext)));
                    }
                }
            }

            outConvertedValues.addAll(values);
        }

        // Selects on non-clustered gsi, need additional look-up
        List<RelNode> selects = new ArrayList<>();
        // Selects on primary table / clustered gsi, get full row
        List<RelNode> primarySelects = new ArrayList<>();

        List<String> selectColumns = selectRowType.getFieldNames();
        List<List<Object>> duplicateValues = new ArrayList<>();

        // List of set that contains each UK's column names
        List<Set<String>> ukSets = new ArrayList<>();
        int totalUk = 0;

        for (Map.Entry<String, List<List<String>>> e : ukGroupByTable.entrySet()) {
            // table that UKs will be searched on, could be primary or gsi
            String currentTableName = e.getKey();

            // New array list to avoid concurrent issue
            // columns that each UK contains
            List<List<String>> currentUkColumnList = new ArrayList<>(e.getValue());
            // each UK's local index name on corresponding table, could be null
            List<String> currentUkNameList = new ArrayList<>(localIndexPhyName.get(currentTableName));

            // If UK includes auto_inc column and using seq, then we can skip looking up duplicate values for this UK.
            // There will be at most 1 auto_inc column in one table.
            if (executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_DUPLICATE_CHECK_FOR_PK)
                && autoIncColumns.size() == 1 && handlerParams.autoIncrementUsingSeq) {
                List<Integer> toRemoved = new ArrayList<>();
                for (int i = 0; i < currentUkColumnList.size(); i++) {
                    if (currentUkColumnList.get(i).stream()
                        .anyMatch(col -> col.equalsIgnoreCase(autoIncColumns.get(0)))) {
                        toRemoved.add(i);
                    }
                }
                for (int i = toRemoved.size() - 1; i >= 0; i--) {
                    currentUkColumnList.remove(toRemoved.get(i).intValue());
                    currentUkNameList.remove(toRemoved.get(i).intValue());
                }
            }
            if (currentUkColumnList.size() == 0) {
                continue;
            }

            final TableMeta currentTableMeta =
                executionContext.getSchemaManager(schemaName).getTable(currentTableName);
            List<List<ColumnMeta>> currentUkColumnMetas = new ArrayList<>();
            for (List<String> l : currentUkColumnList) {
                List<ColumnMeta> columnMetaList = new ArrayList<>();
                for (String columnName : l) {
                    columnMetaList.add(currentTableMeta.getColumnIgnoreCase(columnName));
                }
                currentUkColumnMetas.add(columnMetaList);
            }

            List<Set<String>> currentUkSets = new ArrayList<>();
            for (List<String> uk : currentUkColumnList) {
                final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                for (String columnName : uk) {
                    ColumnMeta columnMeta = currentTableMeta.getColumnIgnoreCase(columnName);
                    if (columnMeta != null && columnMeta.getMappingName() != null && !columnMeta.getMappingName()
                        .isEmpty()) {
                        ukSet.add(columnMeta.getMappingName());
                    } else {
                        ukSet.add(columnName);
                    }
                }
                currentUkSets.add(ukSet);
            }
            ukSets.addAll(currentUkSets);

            // If it's insert ignore, then we do not need to get full row in any case, because UK + PK is enough for
            // deduplication. Otherwise, we should get full row if it's primary table or clustered gsi.
            // For clustered index, we should check if column counts match since we may be in adding column ddl.
            // See https://yuque.antfin.com/coronadb/design/uirwy2
            boolean shouldGetFullRow = !isInsertIgnore && (currentTableName.equalsIgnoreCase(primaryTableName) || (
                currentTableMeta.isClustered()
                    && currentTableMeta.getAllColumns().size() == primaryTableMeta.getPhysicalColumns().size()));
            if (shouldGetFullRow) {
                primarySelects.addAll(
                    buildSelects(insert, lockMode, executionContext, currentTableName, selectColumns, insertColumns,
                        currentUkColumnList, currentUkColumnMetas, currentUkNameList, totalUk, currentUkSets, values,
                        false, false));
            } else {
                selects.addAll(buildSelects(insert, lockMode, executionContext, currentTableName, primaryAndShardingKey,
                    insertColumns, currentUkColumnList, currentUkColumnMetas, currentUkNameList, totalUk, currentUkSets,
                    values, true, false));
            }

            totalUk = ukSets.size();
        }

        List<List<Object>> results;
        if (selects.size() != 0) {
            ExecutionContext selectEc = executionContext.copy();
            selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));
            results = executePhysicalPlan(selects, schemaName, selectEc, memoryAllocator);
        } else {
            results = Collections.emptyList();
        }

        if (isInsertIgnore) {
            List<Pair<Long, Long>> duplicateUk = new ArrayList<>(); // [{valueIndex, ukIndex}]
            results.forEach(r -> duplicateUk.add(new Pair(r.get(0), r.get(1))));
            List<Integer> selectInsertColumnMapping = insert.getSelectInsertColumnMapping();

            for (Pair<Long, Long> p : duplicateUk) {
                List<Object> duplicateValue = new ArrayList<>();
                Set<String> ukSet = ukSets.get(p.right.intValue());
                for (int i = 0; i < selectColumns.size(); i++) {
                    if (ukSet.contains(selectColumns.get(i))) {
                        duplicateValue.add(values.get(p.left.intValue()).get(selectInsertColumnMapping.get(i)));
                    } else {
                        // No need to get full row, so we just fill columns with null
                        duplicateValue.add(null);
                    }
                }
                duplicateValues.add(duplicateValue);
            }
        } else {
            if (!results.isEmpty()) {
                // Group results by UK
                Map<Integer, List<List<Object>>> resultsByUk = new HashMap<>();
                for (List<Object> result : results) {
                    Long ukIndex = (Long) result.get(1);
                    resultsByUk.computeIfAbsent(ukIndex.intValue(), k -> new ArrayList<>()).add(result);
                }

                for (Map.Entry<Integer, List<List<Object>>> e : resultsByUk.entrySet()) {
                    Set<String> currentUkSet = ukSets.get(e.getKey());
                    List<List<Object>> currentValues = new ArrayList<>();

                    // PK and Primary SK are from results, UK is from values
                    for (List<Object> result : e.getValue()) {
                        Long valueIndex = (Long) result.get(0);
                        List<Object> originValue = values.get(valueIndex.intValue());
                        List<Object> currentValue = new ArrayList<>();
                        for (int i = 0; i < insertColumns.size(); i++) {
                            String columnName = insertColumns.get(i);
                            if (currentUkSet.contains(columnName)) {
                                currentValue.add(originValue.get(i));
                            } else if (primaryAndShardingKeyMap.get(columnName) != null) {
                                currentValue.add(result.get(primaryAndShardingKeyMap.get(columnName) + 2));
                            } else {
                                currentValue.add(null);
                            }
                        }
                        currentValues.add(currentValue);
                    }

                    Set<String> columnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    columnSet.addAll(currentUkSet);
                    columnSet.addAll(primaryKey);

                    // use PK + UK as condition
                    List<List<String>> currentUkColumnList = new ArrayList<>();
                    currentUkColumnList.add(new ArrayList<>(columnSet));
                    List<List<ColumnMeta>> currentUkColumnMetas = new ArrayList<>();
                    List<String> currentUkNameList = new ArrayList<>();
                    for (List<String> l : currentUkColumnList) {
                        List<ColumnMeta> columnMetaList = new ArrayList<>();
                        for (String columnName : l) {
                            columnMetaList.add(primaryTableMeta.getColumnIgnoreCase(columnName));
                        }
                        currentUkColumnMetas.add(columnMetaList);
                        currentUkNameList.add("PRIMARY");
                    }

                    List<Set<String>> currentUkSets = new ArrayList<>();
                    for (List<String> uk : currentUkColumnList) {
                        final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                        ukSet.addAll(uk);
                        currentUkSets.add(ukSet);
                    }

                    // Add these to primary selects, and we can not dedup look up values, otherwise we may miss out
                    // some values with same uk in different shards in primary table
                    primarySelects.addAll(
                        buildSelects(insert, lockMode, executionContext, primaryTableName, selectColumns, insertColumns,
                            currentUkColumnList, currentUkColumnMetas, currentUkNameList, totalUk, currentUkSets,
                            currentValues, false, true));
                }
            }

            // Execute
            ExecutionContext selectEc = executionContext.copy();
            selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));
            duplicateValues = executePhysicalPlan(primarySelects, schemaName, selectEc, memoryAllocator);

            // We may get same rows from different table, so we need to dedup duplicateValues in the end to avoid
            // affecting row count.
            List<ColumnMeta> columnMetaList = new ArrayList<>();

            for (String columnName : selectColumns) {
                columnMetaList.add(primaryTableMeta.getColumnIgnoreCase(columnName));
            }

            //只比较主键、uk和分区键（分区键是因为可能主键相同，分区不同），防止包含json列，json暂时无法比较
            boolean[] needCmp = null;
            final Set<String> needCmpColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (executionContext.getParamManager()
                .getBoolean(ConnectionParams.DML_SELECT_SAME_ROW_ONLY_COMPARE_PK_UK_SK)) {
                needCmpColumns.addAll(primaryAndShardingKey);
                for (Set<String> set : ukSets) {
                    needCmpColumns.addAll(set);
                }
                Map<String, Integer> columnNameAndIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (int i = 0; i < selectColumns.size(); i++) {
                    columnNameAndIndex.put(selectColumns.get(i), i);
                }
                needCmp = new boolean[selectColumns.size()];
                for (String column : needCmpColumns) {
                    Integer index = columnNameAndIndex.get(column);
                    if (index != null) {
                        needCmp[index] = true;
                    }
                }
            }

            final boolean[] resultNeedCmp = needCmp;
            Set<GroupKey> checker = new TreeSet<>();
            duplicateValues = duplicateValues.stream().filter(value -> {
                GroupKey groupKey = new GroupKey(value.toArray(), columnMetaList, resultNeedCmp);
                boolean duplicated = checker.contains(groupKey);
                checker.add(groupKey);
                return !duplicated;
            }).collect(Collectors.toList());
        }

        return duplicateValues;
    }

    protected Integer tryPushDownExecute(LogicalInsertIgnore insertIgnore,
                                         String schemaName, String tableName, ExecutionContext insertEc) {

        Integer execAffectRow = null;

        if (insertIgnore.getPushDownInsertWriter() == null) {
            return execAffectRow;
        }

        // 1. Add Switch for the optimization of insert ignore/replace of non-scale-out group
        final boolean scaleOutDmlPushDown =
            insertEc.getParamManager().getBoolean(ConnectionParams.SCALEOUT_DML_PUSHDOWN_OPTIMIZATION);

        // 2. check if exec insert by specifying the execution policy manually.
        //    If execution policy is specified, use the specified execution policy directly.
        //    This check has been done in OptimizeLogicalInsertRule,
        //    if execution policy is specified manually, the pushDownInsertWriter must be null.

        if (scaleOutDmlPushDown) {

            // 3. Get the shard results by Writer
            InsertWriter pushDownInsertWriter = insertIgnore.getPushDownInsertWriter();

            int batchSize = insertIgnore.getBatchSize();
            int pushDownBatchLimit =
                insertEc.getParamManager().getInt(ConnectionParams.SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT);
            if (batchSize > 0 && batchSize > pushDownBatchLimit) {
                // If the batch size is more than the SCALEOUT_DML_PUSHDOWN_BATCH_LIMIT,
                // then ignore the optimization of scaleout push-down
                return execAffectRow;
            }

            List<PhyTableInsertSharder.PhyTableShardResult> shardResults =
                pushDownInsertWriter.getShardResults(insertEc);

            // 4. Check if contain scale-out group for those shard groups
            boolean canExecByPushDownPolicy = false;
            boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
            if (shardResults.size() > 0) {
                Set<String> shardGrpSet = new HashSet<>();
                TableMeta tableMeta = insertEc.getSchemaManager(schemaName).getTable(tableName);
                PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
                for (int i = 0; i < shardResults.size(); i++) {
                    String shardGroup = shardResults.get(i).getGroupName();
                    if (isNewPart) {
                        if (shardGrpSet.size() >= partitionInfo.getPartitionBy().getPartitions().size()) {
                            break;
                        }
                        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                            PartitionLocation partitionLocation = partitionSpec.getLocation();
                            if (partitionLocation.getGroupKey().equalsIgnoreCase(shardGroup) && partitionLocation
                                .getPhyTableName().equalsIgnoreCase(shardResults.get(i).getPhyTableName())) {
                                shardGrpSet.add(partitionSpec.getName());
                            }
                        }
                    } else {
                        shardGrpSet.add(shardGroup.toUpperCase());
                    }
                }
                canExecByPushDownPolicy =
                    !ComplexTaskPlanUtils.checkNeedOpenMultiWrite(schemaName, tableName, shardGrpSet, insertEc);
            }

            if (canExecByPushDownPolicy) {

                // 5. Build physical plan by shard results
                final List<RelNode> replacePlans = pushDownInsertWriter.getInputByShardResults(insertEc, shardResults);

                // 6. Exec the physical plan
                try {
                    execAffectRow = executePhysicalPlan(replacePlans, insertEc, schemaName, true);
                } catch (Throwable e) {
                    handleException(insertEc, e, false);
                }

            }
        }
        return execAffectRow;
    }

    /**
     * Remove duplicated row from current batch parameters
     */
    private static List<Map<Integer, ParameterContext>> getDeduplicatedParams(List<List<ColumnMeta>> ukColumnMetas,
                                                                              List<List<Integer>> ukColumnsListBeforeUkMapping,
                                                                              List<List<Integer>> ukColumnsListAfterUkMapping,
                                                                              LogicalDynamicValues input,
                                                                              List<List<Object>> duplicateValues,
                                                                              List<Map<Integer, ParameterContext>> currentBatchParameters,
                                                                              ExecutionContext executionContext) {
        // Build duplicate checker
        final List<Set<GroupKey>> checkers =
            ExecUtils.buildColumnDuplicateCheckers(duplicateValues, ukColumnsListBeforeUkMapping, ukColumnMetas);

        // Deduplicate batch parameters
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);

        // Ignore last
        return currentBatchParameters.stream().filter(row -> {
            final List<GroupKey> insertRow = ExecUtils
                .buildGroupKeys(ukColumnsListAfterUkMapping, ukColumnMetas,
                    (i) -> RexUtils.getValueFromRexNode(rexRow.get(i), executionContext, row));

            final boolean duplicatedRow = IntStream.range(0, ukColumnsListAfterUkMapping.size()).boxed()
                .anyMatch(i -> ExecUtils.duplicated(checkers.get(i), insertRow.get(i)));

            if (!duplicatedRow) {
                // Duplicate might exists between insert values, add to checker
                Ord.zip(checkers).forEach(o -> o.getValue().add(insertRow.get(o.i)));
            }

            return !duplicatedRow;
        }).collect(Collectors.toList());
    }

    private static List<Map<Integer, ParameterContext>> getDeduplicatedParamsWithNewGroupKey(
        List<List<ColumnMeta>> ukColumnMetas,
        List<List<Integer>> ukColumnsListBeforeUkMapping,
        List<List<Integer>> ukColumnsListAfterUkMapping,
        List<List<Object>> duplicateValues,
        List<List<Object>> convertedValues,
        List<Map<Integer, ParameterContext>> currentBatchParameters,
        ExecutionContext executionContext) {
        // Build duplicate checker
        final List<Set<GroupKey>> checkers =
            ExecUtils.buildColumnDuplicateCheckersWithNewGroupKey(duplicateValues, ukColumnsListBeforeUkMapping,
                ukColumnMetas, executionContext);

        List<Map<Integer, ParameterContext>> results = new ArrayList<>();

        // Ignore last
        for (int j = 0; j < currentBatchParameters.size(); j++) {
            Map<Integer, ParameterContext> row = currentBatchParameters.get(j);
            final List<GroupKey> insertRow = ExecUtils.buildNewGroupKeys(ukColumnsListAfterUkMapping, ukColumnMetas,
                convertedValues.get(j)::get, executionContext);
            final boolean duplicatedRow = IntStream.range(0, ukColumnsListAfterUkMapping.size()).boxed()
                .anyMatch(i -> ExecUtils.duplicated(checkers.get(i), insertRow.get(i)));

            if (!duplicatedRow) {
                // Duplicate might exists between insert values, add to checker
                Ord.zip(checkers).forEach(o -> o.getValue().add(insertRow.get(o.i)));
                results.add(row);
            }
        }

        return results;
    }

    protected static boolean identicalRow(List<Object> before, List<Object> after, List<ColumnMeta> rowColumnMetas,
                                          boolean checkJsonByStringCompare) {
        final GroupKey beforeKey = new GroupKey(before.toArray(), rowColumnMetas);
        final GroupKey afterKey = new GroupKey(after.toArray(), rowColumnMetas);
        // should use equalsForUpdate or may get wrong result when different types
        return beforeKey.equalsForUpdate(afterKey, checkJsonByStringCompare);
    }

    private boolean isGsiCanUseReturning(RelOptTable primary, ExecutionContext ec) {
        final boolean gsiCanUseReturning = GlobalIndexMeta
            .isAllGsi(primary, ec, GlobalIndexMeta::canWrite) &&
            !ComplexTaskPlanUtils.isAnyUGsi(primary, ec, ComplexTaskPlanUtils::isBackfillInProgress);
        return gsiCanUseReturning;
    }
}
