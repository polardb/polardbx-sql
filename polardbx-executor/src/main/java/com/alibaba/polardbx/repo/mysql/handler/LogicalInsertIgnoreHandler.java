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
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
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
        final LogicalInsertIgnore insertIgnore = (LogicalInsertIgnore) insert;
        final String schemaName = insertIgnore.getSchemaName();
        final String tableName = insertIgnore.getLogicalTableName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(tableName);
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        int affectRows = 0;
        // For batch insertIgnore, change params index.
        if (insertIgnore.getBatchSize() > 0) {
            insertIgnore.buildParamsForBatch(executionContext.getParams());
        }

        final boolean gsiConcurrentWrite =
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, gsiConcurrentWrite);

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
        final RelDataType rowType = getRowTypeForDuplicateCheck(insertIgnore);

        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        final boolean allDnUseXDataSource = isAllDnUseXDataSource(topologyHandler);
        final boolean gsiCanUseReturning = GlobalIndexMeta
            .isAllGsi(insertIgnore.getTargetTables().get(0), executionContext, GlobalIndexMeta::canWrite);

        boolean canUseReturning =
            executorContext.getStorageInfoManager().supportsReturning() && executionContext.getParamManager()
                .getBoolean(ConnectionParams.DML_USE_RETURNING) && allDnUseXDataSource && gsiCanUseReturning
                && !isBroadcast && !ComplexTaskPlanUtils.canWrite(tableMeta);

        if (canUseReturning) {
            canUseReturning = noDuplicateValues(insertIgnore, insertEc);
        }

        if (canUseReturning) {
            // Optimize by insert ignore returning

            final List<RelNode> allPhyPlan =
                new ArrayList<>(replaceSeqAndBuildPhyPlan(insertIgnore, insertEc, handlerParams));
            getPhysicalPlanForGsi(insertIgnore.getGsiInsertIgnoreWriters(), insertEc, allPhyPlan);

            // Mix execute
            final Map<String, List<List<Object>>> tableInsertedValues =
                executeAndGetReturning(executionContext, allPhyPlan, insertIgnore, insertEc, memoryAllocator,
                    rowType);

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

            if (returnIgnored) {
                return ignoredRows;
            } else {
                return affectedRows;
            }
        } else {
            handlerParams.optimizedWithReturning = false;
        }

        try {
            // Duplicate might exists between insert values
            // Get and lock duplicated values
            final List<List<Object>> duplicateValues =
                getDuplicatedValues(insertIgnore, LockMode.SHARED_LOCK, executionContext, handlerParams,
                    (rowCount) -> memoryAllocator
                        .allocateReservedMemory(MemoryEstimator.calcSelectValuesMemCost(rowCount, rowType)));
            // Get deduplicated batch parameters
            final List<Map<Integer, ParameterContext>> batchParameters =
                executionContext.getParams().getBatchParameters();
            // Duplicate might exists between insert values
            final List<Map<Integer, ParameterContext>> deduplicated =
                getDeduplicatedParams(insertIgnore, duplicateValues, batchParameters, executionContext);

            if (!deduplicated.isEmpty()) {
                insertEc.setParams(new Parameters(deduplicated));
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
        return affectRows;
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
            final Set<GroupKey> insertedPks = new HashSet<>();
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
        final Set<GroupKey> distinctPks = new HashSet<>();
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

    private boolean noDuplicateValues(LogicalInsertIgnore insertIgnore, ExecutionContext insertEc) {
        final List<List<ColumnMeta>> ukColumnMetas = insertIgnore.getUkColumnMetas();
        final List<List<Integer>> ukColumnsListAfterUkMapping = insertIgnore.getAfterUkMapping();

        final LogicalDynamicValues input = RelUtils.getRelInput(insertIgnore);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final List<Map<Integer, ParameterContext>> parameters = insertEc.getParams().getBatchParameters();
        final List<Set<GroupKey>> checkers = new ArrayList<>(ukColumnMetas.size());
        IntStream.range(0, ukColumnMetas.size()).forEach(i -> checkers.add(new HashSet<>()));

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

            if (duplicateExists) {
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
            }
            if (inputs.size() == 1) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;
            }

            final List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(insertEc, inputs, queryConcurrencyPolicy, inputCursors, schemaName);

            // Increase physical sql id
            executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);

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
                        for (Object v : rawValues) {
                            outValues.add(DataTypeUtil.toJavaObject(v));
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

    /**
     * Get duplicated values for INSERT IGNORE, INSERT ON DUPLICATE KEY UPDATE and REPLACE
     *
     * @param insertOrReplace INSERT or REPLACE statement
     * @param lockMode Add lock while scan for duplicate
     * @param executionContext ExecutionContext
     * @param memoryAllocator Memory quota allocator
     * @return Duplicated values
     */
    protected List<List<Object>> getDuplicatedValues(LogicalInsertIgnore insertOrReplace, LockMode lockMode,
                                                     ExecutionContext executionContext,
                                                     LogicalInsert.HandlerParams handlerParams,
                                                     Consumer<Integer> memoryAllocator) {
        final String schemaName = insertOrReplace.getSchemaName();
        final String tableName = insertOrReplace.getLogicalTableName();

        // Get plan for finding duplicate values
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert oc != null;
        final TableMeta baseTableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        // Get entire row out
        final List<String> selectKeyNames = insertOrReplace.getSelectListForDuplicateCheck();
        final List<List<String>> ukColumnsList = insertOrReplace.getUkColumnNamesList();
        final List<List<ColumnMeta>> ukColumnMetas = insertOrReplace.getUkColumnMetas();

        // Get duplicate values
        final ExecutionContext selectEc = executionContext.copy();
        final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);

        final List<Map<GroupKey, List<Object>>> deduplicated = new ArrayList<>(ukColumnsList.size());
        IntStream.range(0, ukColumnsList.size()).forEach(i -> deduplicated.add(new LinkedHashMap<>()));

        final Map<String, Integer> columnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(input.getRowType().getFieldNames()).forEach(o -> columnIndexMap.put(o.getValue(), o.getKey()));

        final List<List<Integer>> valuesUkMapping = new ArrayList<>(ukColumnsList.size());
        for (final List<String> ukColumns : ukColumnsList) {
            final List<Integer> valueUkColumnIndexes = new ArrayList<>(ukColumns.size());
            for (String ukColumn : ukColumns) {
                valueUkColumnIndexes.add(columnIndexMap.get(ukColumn));
            }
            valuesUkMapping.add(valueUkColumnIndexes);
        }

        // Deduplicate uk param
        for (Map<Integer, ParameterContext> parameterRow : selectEc.getParams().getBatchParameters()) {
            input.getTuples().forEach(row -> {
                final List<GroupKey> groupKeys = ExecUtils.buildGroupKeys(valuesUkMapping, ukColumnMetas,
                    (i) -> RexUtils.getValueFromRexNode(row.get(i), selectEc, parameterRow));
                IntStream.range(0, valuesUkMapping.size()).forEach(i -> deduplicated.get(i).put(groupKeys.get(i),
                    Arrays.asList(groupKeys.get(i).getGroupKeys())));
            });
        }

        boolean fullTableScan = true;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_DUPLICATE_CHECK_FOR_PK)) {
            final boolean singleOrBroadcast =
                Optional.ofNullable(oc.getRuleManager()).map(rule -> !rule.isShard(tableName)).orElse(true);
            // If using sequence for primary key, we can skip checking duplicate for primary key columns
            fullTableScan = !GlobalIndexMeta.isEveryUkContainsPrimaryPartitionKey(baseTableMeta,
                GlobalIndexMeta.getTableUkMap(baseTableMeta, !handlerParams.autoIncrementUsingSeq, executionContext))
                || singleOrBroadcast;
        }

        List<RelNode> selects;
        if (fullTableScan) {
            final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
            selects = builder
                .buildSelectUnionAndParam(insertOrReplace, selectEc, selectKeyNames, ukColumnsList, baseTableMeta,
                    lockMode, deduplicated);
        } else {
            // Partition pruning
            final InsertWriter primaryInsertWriter = insertOrReplace.getPrimaryInsertWriter();
            final List<RelNode> oldPhysicalPlans = primaryInsertWriter.getInput(selectEc, true);

            final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
            selects = builder.buildSelect(baseTableMeta, oldPhysicalPlans, selectKeyNames, ukColumnsList);
        }

        // Remove batch flag
        selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));

        return executePhysicalPlan(selects, schemaName, selectEc, memoryAllocator);
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
    private static List<Map<Integer, ParameterContext>> getDeduplicatedParams(LogicalInsertIgnore insertIgnore,
                                                                              List<List<Object>> duplicateValues,
                                                                              List<Map<Integer, ParameterContext>> currentBatchParameters,
                                                                              ExecutionContext executionContext) {
        final List<List<ColumnMeta>> ukColumnMetas = insertIgnore.getUkColumnMetas();
        final List<List<Integer>> ukColumnsListBeforeUkMapping = insertIgnore.getBeforeUkMapping();
        final List<List<Integer>> ukColumnsListAfterUkMapping = insertIgnore.getAfterUkMapping();

        // Build duplicate checker
        final List<Set<GroupKey>> checkers =
            ExecUtils.buildColumnDuplicateCheckers(duplicateValues, ukColumnsListBeforeUkMapping, ukColumnMetas);

        // Deduplicate batch parameters
        final LogicalDynamicValues input = RelUtils.getRelInput(insertIgnore);
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

    protected static boolean identicalRow(List<Object> before, List<Object> after, List<ColumnMeta> rowColumnMetas) {
        final GroupKey beforeKey = new GroupKey(before.toArray(), rowColumnMetas);
        final GroupKey afterKey = new GroupKey(after.toArray(), rowColumnMetas);
        return ExecUtils.duplicated(beforeKey, afterKey, true);
    }
}
