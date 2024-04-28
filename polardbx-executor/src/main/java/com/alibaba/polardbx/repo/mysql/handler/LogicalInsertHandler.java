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

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.InsertIndexExecutor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert.HandlerParams;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithSomethingVisitor;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryControlByBlocked;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.repo.mysql.handler.execute.ExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.InsertSelectExecuteJob;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_DUP_ENTRY;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DUP_ENTRY;
import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;
import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

/**
 * Created by minggong.zm on 18/1/16.
 */
public class LogicalInsertHandler extends HandlerCommon {

    public LogicalInsertHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        // Need auto-savepoint only when auto-commit = 0.
        executionContext.setNeedAutoSavepoint(!executionContext.isAutoCommit());
        HandlerParams handlerParams = new HandlerParams();

        LogicalInsert logicalInsert = (LogicalInsert) logicalPlan;
        checkInsertLimitation(logicalInsert, executionContext);
        if (!logicalInsert.isSourceSelect()) {
            RexUtils.calculateAndUpdateAllRexCallParams(logicalInsert, executionContext);
        }
        final long oldLastInsertId = executionContext.getConnection().getLastInsertId();
        int affectRows = 0;
        try {
            if (!logicalInsert.isSourceSelect()) {
                affectRows = doExecute(logicalInsert, executionContext, handlerParams);
            } else {
                affectRows = insertSelectHandle(logicalInsert, executionContext, handlerParams);
            }
        } catch (Throwable e) {
            // If exception happens, reset last insert id.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
            throw GeneralUtil.nestedException(e);
        }

        // If it's a single table, only MyJdbcHandler knows last insert id, and
        // it writes the value into Connection.
        // If it's a sharded table, correct last insert id is in LogicalInsert,
        // so overwrite the value MyJdbcHandler wrote.
        if (handlerParams.returnedLastInsertId != 0) {
            executionContext.getConnection().setReturnedLastInsertId(handlerParams.returnedLastInsertId);
        }
        if (handlerParams.lastInsertId != 0) {
            // Using sequence, override the value set by MyJdbcHandler
            executionContext.getConnection().setLastInsertId(handlerParams.lastInsertId);
        } else if (handlerParams.usingSequence) {
            // Using sequence, but all auto increment column values are specified.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
        } else {
            // Not using sequence. Use the value set by MyJdbcHandler.
        }
        executionContext.setOptimizedWithReturning(handlerParams.optimizedWithReturning);
        return new AffectRowCursor(affectRows);
    }

    protected int doExecute(LogicalInsert logicalInsert, ExecutionContext executionContext,
                            HandlerParams handlerParams) {
        final RelNode input = logicalInsert.getInput();
        if (input instanceof LogicalDynamicValues && logicalInsert.getBatchSize() > 0) {
            // For batch insert, change params index.
            logicalInsert.buildParamsForBatch(executionContext);
        }
        return executeInsert(logicalInsert, executionContext, handlerParams);
    }

    /**
     * If it's using MySQL auto increment column instead of Sequence, throw an
     * exception. If it lacks some necessary columns, e.g. sharding key, throw
     * an exception.
     */
    protected void checkInsertLimitation(LogicalInsert logicalInsert, ExecutionContext executionContext) {
        if (!logicalInsert.isInsert() && !logicalInsert.isReplace()) {
            return;
        }

        String schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        String targetTableName = logicalInsert.getLogicalTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTableName);

        ITransactionPolicy trxPolicy = executionContext.getConnection().getTrxPolicy();
    }

    /**
     * Do physical insertion, return affect rows.
     *
     * @return affectRows
     */
    protected int executeInsert(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                HandlerParams handlerParams) {
        String schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final String tableName = logicalInsert.getLogicalTableName();
        final boolean isBroadcast = or.isBroadCast(tableName);

        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);
        // Could be other deterministic pushdown DML, which does not need to be checked here
        final boolean checkPrimaryKey = logicalInsert.isSimpleInsert() &&
            executionContext.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK)
            && !logicalInsert.isPushablePrimaryKeyCheck();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        final boolean checkForeignKey = logicalInsert.isSimpleInsert() &&
            executionContext.foreignKeyChecks() && tableMeta.hasForeignKey();

        if (null != logicalInsert.getPrimaryInsertWriter() && !logicalInsert.hasHint() && executionContext
            .getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE)) {

            RexUtils.updateParam(logicalInsert, executionContext, true, handlerParams);

            if (checkPrimaryKey || checkForeignKey) {
                beforeInsertCheck(logicalInsert, checkPrimaryKey, checkForeignKey, executionContext);
            }

            // Get plan for primary
            final InsertWriter primaryWriter = logicalInsert.getPrimaryInsertWriter();
            List<RelNode> inputs = primaryWriter.getInput(executionContext);
            final List<RelNode> primaryPhyPlan =
                inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());

            final List<RelNode> allPhyPlan = new ArrayList<>(primaryPhyPlan);
            final List<RelNode> replicatePhyPlan =
                inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());

            allPhyPlan.addAll(replicatePhyPlan);

            // Get plan for gsi
            final AtomicInteger writableGsiCount = new AtomicInteger(0);
            final List<InsertWriter> gsiWriters = logicalInsert.getGsiInsertWriters();
            gsiWriters.stream()
                .map(gsiWriter -> gsiWriter.getInput(executionContext))
                .filter(w -> !w.isEmpty())
                .forEach(w -> {
                    writableGsiCount.incrementAndGet();
                    allPhyPlan.addAll(w);
                });

            // Test Code for test shardValues of partition table
            /*
            try {
                String logTbName = logicalInsert.getLogicalTableName();
                TableMeta meta = OptimizerContext.getContext(schemaName).getSchemaManager().getTable(logTbName);
                Map<String, Map<String, List<Integer>>> rs = BuildPlanUtils
                    .shardValues((SqlInsert) logicalInsert.getSqlTemplate(), meta, executionContext, schemaName, null);
                System.out.print(rs);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
            */

            // Enable gsi concurrent write
            executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, true);

            try {
                // Default concurrent policy is group concurrent
                final int totalAffectRows = executePhysicalPlan(allPhyPlan, executionContext, schemaName, isBroadcast);
                boolean multiWriteWithoutBroadcast =
                    (writableGsiCount.get() > 0 || GeneralUtil.isNotEmpty(replicatePhyPlan)) && !isBroadcast;
                boolean multiWriteWithBroadcast =
                    (writableGsiCount.get() > 0 || GeneralUtil.isNotEmpty(replicatePhyPlan)) && isBroadcast;

                if (multiWriteWithoutBroadcast) {
                    return primaryPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows())
                        .sum();
                } else if (multiWriteWithBroadcast) {
                    return ((BaseQueryOperation) primaryPhyPlan.get(0)).getAffectedRows();
                } else {
                    return totalAffectRows;
                }
            } catch (Throwable e) {
                if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_CRUCIAL_ERR_CHECK)
                    || executionContext.isModifyBroadcastTable() || executionContext.isModifyGsiTable()) {
                    // Can't commit
                    executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL,
                        e.getMessage());
                }
                throw GeneralUtil.nestedException(e);
            }
        } else {
            executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, false);
        }

        // TODO(qianjing): should we check PK and FK when GSI_CONCURRENT_WRITE is false?
        List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>();
        PhyTableInsertSharder insertSharder = new PhyTableInsertSharder(logicalInsert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
        List<RelNode> inputs = logicalInsert.getInput(insertSharder, shardResults, executionContext);

        handlerParams.usingSequence = insertSharder.isUsingSequence();
        handlerParams.lastInsertId = insertSharder.getLastInsertId();
        handlerParams.returnedLastInsertId = insertSharder.getReturnedLastInsertId();

        assert shardResults.size() == inputs.size();
        if (!logicalInsert.hasHint() && executionContext.getParams() != null
            && GlobalIndexMeta.hasIndex(tableName, schemaName, executionContext)) {
            executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.SQL.metricLevel);
            return executeIndex(tableName,
                insertSharder.getSqlTemplate(),
                logicalInsert,
                inputs,
                shardResults,
                executionContext,
                schemaName);
        } else {
            List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            QueryConcurrencyPolicy queryConcurrencyPolicy =
                inputs.size() > 1 ? getQueryConcurrencyPolicy(executionContext) : QueryConcurrencyPolicy.SEQUENTIAL;
            executeWithConcurrentPolicy(executionContext, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            return ExecUtils.getAffectRowsByCursors(inputCursors, isBroadcast);
        }
    }

    protected void beforeInsertCheck(LogicalInsert logicalInsert, boolean checkPk, boolean checkFk,
                                     ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(logicalInsert.getSchemaName())
            .getTable(logicalInsert.getLogicalTableName());
        if (!tableMeta.hasForeignKey()) {
            return;
        }
        final LogicalDynamicValues input = RelUtils.getRelInput(logicalInsert);
        List<List<Object>> values =
            getInputValues(input, executionContext.getParams().getBatchParameters(), executionContext);
        List<String> insertColumns = input.getRowType().getFieldNames().stream().map(String::toUpperCase).collect(
            Collectors.toList());
        beforeInsertCheck(logicalInsert, values, insertColumns, checkPk, checkFk, executionContext);
    }

    protected void beforeInsertCheck(LogicalInsert logicalInsert, List<List<Object>> values, List<String> insertColumns,
                                     boolean checkPk, boolean checkFk, ExecutionContext executionContext) {
        if (checkPk) {
            checkPrimaryConstraint(logicalInsert, values, insertColumns, executionContext);
        }

        if (checkFk) {
            beforeInsertFkCheck(logicalInsert, logicalInsert.getLogicalTableName(), executionContext, values);
        }
    }

    protected void beforeInsertFkCheck(LogicalInsert logicalInsert, String targetTable,
                                       ExecutionContext executionContext, List<List<Object>> values) {
        String schemaName = logicalInsert.getSchemaName();
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTable);

        ExecutionContext selectEc = executionContext.copy();
        selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final List<Pair<String, ForeignKeyData>> foreignKeysWithIndex =
            tableMeta.getForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());

        List<String> insertColumns = logicalInsert.getInsertRowType().getFieldNames();

        for (Pair<String, ForeignKeyData> data : foreignKeysWithIndex) {
            if (data.getValue().isPushDown()) {
                continue;
            }

            schemaName = data.getValue().refSchema;
            String tableName = data.getValue().refTableName;

            PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, selectEc);
            final TableMeta parentTableMeta = selectEc.getSchemaManager(schemaName).getTableWithNull(tableName);
            if (parentTableMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_UPDATE_FK_CONSTRAINT, schemaName, tableName,
                    data.getValue().toString());
            }

            final int maxSqlUnionCount =
                executionContext.getParamManager().getInt(ConnectionParams.DML_GET_DUP_UNION_SIZE);

            List<List<List<Object>>> partitions = new ArrayList<>();

            for (int i = 0; i < values.size(); i += maxSqlUnionCount) {
                partitions.add(values.subList(i, Math.min(i + maxSqlUnionCount, values.size())));
            }

            for (List<List<Object>> partition : partitions) {
                List<List<Object>> conditionValueList =
                    getConditionValueList(data.getValue(), tableMeta, partition, null, insertColumns, true);

                Map<String, String> columnMap =
                    IntStream.range(0, data.getValue().columns.size()).collect(TreeMaps::caseInsensitiveMap,
                        (m, i) -> m.put(data.getValue().columns.get(i), data.getValue().refColumns.get(i)),
                        Map::putAll);

                List<String> sortedColumns = new ArrayList<>();
                insertColumns.forEach(c -> {
                    if (columnMap.containsKey(c)) {
                        sortedColumns.add(columnMap.get(c));
                    }
                });

                Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
                    getShardResults(data.getValue(), schemaName, tableName, tableMeta, parentTableMeta,
                        conditionValueList,
                        builder, sortedColumns, true, true);

                conditionValueList = conditionValueList.stream().distinct().collect(Collectors.toList());

                // continue if contains null (MATCH SIMPLE)
                conditionValueList.removeIf(conditionValue -> conditionValue.contains(null));
                if (conditionValueList.isEmpty()) {
                    return;
                }

                List<List<Object>> selectValues = getSelectValues(selectEc, schemaName,
                    parentTableMeta, conditionValueList, logicalInsert, memoryAllocator, builder, shardResults,
                    sortedColumns, true);

                List<List<Object>> distinctSelectValues = selectValues.stream().distinct().collect(Collectors.toList());

                if (distinctSelectValues.size() != conditionValueList.size()) {
                    if (data.getValue().refTableName.equals(targetTable)) {
                        if (!skipFkSameTable(data.getValue(), tableMeta, partition)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ADD_UPDATE_FK_CONSTRAINT,
                                data.getValue().refSchema,
                                tableName,
                                data.getValue().toString());
                        }

                        return;
                    }

                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_UPDATE_FK_CONSTRAINT, data.getValue().refSchema,
                        tableName,
                        data.getValue().toString());
                }
            }
        }
    }

    protected List<Map<Integer, ParameterContext>> beforeInsertFkCheckIgnore(LogicalInsert logicalInsert,
                                                                             String targetTable,
                                                                             ExecutionContext executionContext,
                                                                             List<List<Object>> values,
                                                                             List<Map<Integer, ParameterContext>> deduplicated) {
        List<Map<Integer, ParameterContext>> result = new ArrayList<>();
        Set<Integer> indexes = IntStream.rangeClosed(0, values.size() - 1)
            .boxed().collect(Collectors.toSet());

        final String schemaName = logicalInsert.getSchemaName();
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTable);

        ExecutionContext selectEc = executionContext.copy();
        selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final List<Pair<String, ForeignKeyData>> foreignKeysWithIndex =
            tableMeta.getForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, selectEc);

        List<String> insertColumns = logicalInsert.getInsertRowType().getFieldNames();

        for (Pair<String, ForeignKeyData> data : foreignKeysWithIndex) {
            String tableName = data.getValue().refTableName;
            final TableMeta parentTableMeta = selectEc.getSchemaManager(schemaName).getTable(tableName);

            for (int i = 0; i < values.size(); i++) {
                List<List<Object>> singleValues = new ArrayList<>();
                singleValues.add(values.get(i));

                List<List<Object>> conditionValueList =
                    getConditionValueList(data.getValue(), tableMeta, singleValues, null, insertColumns, true);

                // continue if contains null
                if (conditionValueList.get(0).contains(null)) {
                    continue;
                }

                Map<String, String> columnMap = IntStream.range(0, data.getValue().columns.size()).collect(HashMap::new,
                    (m, n) -> m.put(data.getValue().columns.get(n), data.getValue().refColumns.get(n)),
                    Map::putAll);

                List<String> sortedColumns = new ArrayList<>();
                insertColumns.forEach(c -> {
                    if (columnMap.containsKey(c)) {
                        sortedColumns.add(columnMap.get(c));
                    }
                });

                Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
                    getShardResults(data.getValue(), schemaName, tableName, tableMeta, parentTableMeta,
                        conditionValueList,
                        builder, sortedColumns, true, true);

                List<List<Object>> selectValues = getSelectValues(selectEc, schemaName,
                    parentTableMeta, conditionValueList, logicalInsert, memoryAllocator, builder, shardResults,
                    sortedColumns, false);

                if (selectValues.isEmpty()) {
                    List<List<Object>> v = new ArrayList<>();
                    v.add(values.get(i));
                    if (data.getValue().refTableName.equals(targetTable) &&
                        skipFkSameTable(data.getValue(), tableMeta, v)) {
                        continue;
                    }

                    indexes.remove(i);
                }
            }
        }
        for (int i = 0; i < deduplicated.size(); i++) {
            if (indexes.contains(i)) {
                result.add(deduplicated.get(i));
            }
        }
        return result;
    }

    private boolean skipFkSameTable(ForeignKeyData data, TableMeta tableMeta, List<List<Object>> values) {
        boolean skipFkSameTable = true;
        List<Pair<Integer, Integer>> fkColumnNumbers = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        tableMeta.getAllColumns().stream().forEach(c -> columnNames.add(c.getName().toLowerCase()));
        for (int i = 0; i < data.columns.size(); i++) {
            fkColumnNumbers.add(new Pair<>(columnNames.indexOf(data.columns.get(i).toLowerCase()),
                columnNames.indexOf(data.refColumns.get(i))));
        }
        for (Pair<Integer, Integer> fkColumnNumber : fkColumnNumbers) {
            Set<Object> refCol = new HashSet<>();
            values.stream().forEach(value -> refCol.add(value.get(fkColumnNumber.right)));
            for (List<Object> value : values) {
                if (!refCol.contains(value.get(fkColumnNumber.left))) {
                    skipFkSameTable = false;
                    break;
                }
            }
        }
        return skipFkSameTable;
    }

    /**
     * Build selects for PK/UK/FK Check
     */
    protected List<RelNode> buildSelects(LogicalInsert insert, SqlSelect.LockMode lockMode,
                                         ExecutionContext executionContext, String tableName,
                                         List<String> selectColumns, List<String> insertColumns,
                                         List<List<String>> ukColumnsList, List<List<ColumnMeta>> ukColumnMetas,
                                         List<String> ukNameList, int ukIndexOffset, List<Set<String>> currentUkSets,
                                         List<List<Object>> values, boolean withValueIndex,
                                         boolean includingShardingKey, boolean dedupLookUpValue) {
        final String schemaName = insert.getSchemaName();
        final String primaryTableName = insert.getLogicalTableName();

        // Get plan for finding duplicate values
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert oc != null;
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

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

        final List<List<List<Object>>> lookUpUniqueKey = new ArrayList<>();
        final List<List<Pair<Integer, Integer>>> lookUpUniqueKeyIndex = new ArrayList<>();

        IntStream.range(0, ukColumnsList.size()).forEach(i -> lookUpUniqueKey.add(new ArrayList<>()));
        IntStream.range(0, ukColumnsList.size()).forEach(i -> lookUpUniqueKeyIndex.add(new ArrayList<>()));

        if (!dedupLookUpValue) {
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

        // If includingShardingKey is true, then there is no need to do fullTableScan since we have selected primary
        // sharding key in previous step
        // And we can not use insertColumns to check since it may be filled with null, or it's a different table
        boolean fullTableScan =
            singleOrBroadcast || (!GlobalIndexMeta.isEveryUkContainsTablePartitionKey(tableMeta, currentUkSets)
                && !includingShardingKey);

        List<RelNode> selects;
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
        final int maxSqlUnionCount = executionContext.getParamManager().getInt(ConnectionParams.DML_GET_DUP_UNION_SIZE);
        final boolean useIn = executionContext.getParamManager().getBoolean(ConnectionParams.DML_GET_DUP_USING_IN);
        final int maxSqlInCount = executionContext.getParamManager().getInt(ConnectionParams.DML_GET_DUP_IN_SIZE);
        // Use IN instead of UNION may cause more deadlocks
        // Ref: https://dev.mysql.com/doc/refman/5.7/en/innodb-locks-set.html
        // If it's select with value index, we can not use IN because each select has its own value index

        selects = withValueIndex || !useIn ?
            builder.buildSelectUnionAndParam(insert, ukColumnsList, tableMeta, lockMode, values, insertColumns,
                lookUpUniqueKey, lookUpUniqueKeyIndex, selectColumns, withValueIndex, maxSqlUnionCount, fullTableScan) :
            builder.buildSelectInAndParam(insert, ukColumnsList, ukNameList, tableMeta, lockMode, values,
                insertColumns, lookUpUniqueKey, lookUpUniqueKeyIndex, selectColumns, maxSqlInCount, fullTableScan);

        return selects;
    }

    private void checkPrimaryConstraint(LogicalInsert insert, List<List<Object>> values, List<String> insertColumns,
                                        ExecutionContext executionContext) {
        final String schemaName = insert.getSchemaName();
        final String primaryTableName = insert.getLogicalTableName();
        TableMeta primaryTableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
        List<String> primaryKey = GlobalIndexMeta.getPrimaryKeys(primaryTableMeta).stream().map(String::toUpperCase)
            .collect(Collectors.toList());
        List<ColumnMeta> primaryColumnMetas =
            primaryKey.stream().map(primaryTableMeta::getColumn).collect(Collectors.toList());
        List<Set<String>> currentUkSets = new ArrayList<>();
        final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        ukSet.addAll(primaryKey);
        currentUkSets.add(ukSet);

        List<RelNode> selects =
            buildSelects(insert, SqlSelect.LockMode.SHARED_LOCK, executionContext, primaryTableName, primaryKey,
                insertColumns, ImmutableList.of(primaryKey), ImmutableList.of(primaryColumnMetas),
                ImmutableList.of("PRIMARY"), 0, currentUkSets, values, false, false, true);
        ExecutionContext selectEc = executionContext.copy();
        selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        RelDataType selectRowType = getRowTypeForColumns(insert,
            executionContext.getSchemaManager(schemaName).getTable(primaryTableName).getPrimaryKey());

        // results only contain PK
        List<List<Object>> results = executePhysicalPlan(selects, schemaName, selectEc, (rowCount) -> memoryAllocator
            .allocateReservedMemory(MemoryEstimator.calcSelectValuesMemCost(rowCount, selectRowType)));
        List<Integer> beforeMapping = IntStream.range(0, primaryKey.size()).boxed().collect(Collectors.toList());
        List<Integer> afterMapping = new ArrayList<>();
        for (int i = 0; i < primaryKey.size(); i++) {
            int pos = -1;
            for (int j = 0; j < insertColumns.size(); j++) {
                if (primaryKey.get(i).equals(insertColumns.get(j))) {
                    pos = j;
                    break;
                }
            }
            afterMapping.add(pos);
        }

        // Build duplicate checker
        final List<Set<GroupKey>> checkers =
            ExecUtils.buildColumnDuplicateCheckers(results, ImmutableList.of(beforeMapping),
                ImmutableList.of(primaryColumnMetas));

        // Deduplicate batch parameters
        for (List<Object> value : values) {
            final List<GroupKey> insertRow =
                ExecUtils.buildGroupKeys(ImmutableList.of(afterMapping), ImmutableList.of(primaryColumnMetas),
                    value::get);

            final boolean duplicatedRow = ExecUtils.duplicated(checkers.get(0), insertRow.get(0));

            if (duplicatedRow) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Cannot insert row: duplicate primary key found for row (" + value.stream().map(String::valueOf)
                        .collect(Collectors.joining(",")) + ")");
            }
            // Duplicate might exists between insert values, add to checker
            Ord.zip(checkers).forEach(o -> o.getValue().add(insertRow.get(o.i)));
        }
    }

    @Deprecated
    protected Set<Integer> checkForeignConstraint(LogicalInsert insert, List<List<Object>> values,
                                                  List<String> insertColumns, ExecutionContext ec) {
        final String schemaName = insert.getSchemaName();
        final String targetTable = insert.getLogicalTableName();
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert oc != null;
        final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(targetTable);

        Set<Integer> violatedValues = new HashSet<>();

        if (!tableMeta.hasForeignKey()) {
            return violatedValues;
        }

        final Map<String, Integer> insertColumnIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < insertColumns.size(); i++) {
            insertColumnIndex.put(insertColumns.get(i), i);
        }

        // Build select row type
        RelDataTypeFactory typeFactory = insert.getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("value_index", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("uk_index", 1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        final RelDataType selectRowType = typeFactory.createStructType(columns);

        // Assign each foreign key an index
        final List<Pair<String, ForeignKeyData>> foreignKeysWithIndex =
            tableMeta.getForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());

        final String primaryTableName = insert.getLogicalTableName();
        TableMeta primaryTableMeta = ec.getSchemaManager(schemaName).getTable(primaryTableName);
        List<String> primaryKey = GlobalIndexMeta.getPrimaryKeys(primaryTableMeta);

        int totalFk = 0;

        // Selects on non-clustered gsi, need additional look-up
        final List<RelNode> selects = new ArrayList<>();

        final List<Map<String, String>> fkTarColToRefColMapping = new ArrayList<>();
        for (Pair<String, ForeignKeyData> o : foreignKeysWithIndex) {
            final ForeignKeyData fkData = o.getValue();
            final Map<String, String> colMapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Ord<String> c : Ord.zip(fkData.refColumns)) {
                final Integer colIndex = c.getKey();
                final String refCol = c.getValue();
                final String tarCol = fkData.columns.get(colIndex);
                colMapping.put(tarCol, refCol);
            }
            fkTarColToRefColMapping.add(colMapping);
        }

        final Map<String, Map<Integer, Pair<List<String>, List<String>>>> fkGroupByRefTable =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Ord<Pair<String, ForeignKeyData>> o : Ord.zip(foreignKeysWithIndex)) {
            final Integer fkIndex = o.getKey();
            final ForeignKeyData fkData = o.getValue().getValue();
            boolean skipFkSameTable = false;
            if (fkData.refTableName.equals(targetTable)) {
                skipFkSameTable = true;
                List<Pair<Integer, Integer>> fkColumnNumbers = new ArrayList<>();
                List<String> columnNames = new ArrayList<>();
                tableMeta.getAllColumns().stream().forEach(c -> columnNames.add(c.getName()));
                for (int i = 0; i < fkData.columns.size(); i++) {
                    fkColumnNumbers.add(new Pair<>(columnNames.indexOf(fkData.columns.get(i)),
                        columnNames.indexOf(fkData.refColumns.get(i))));
                }
                for (List<Object> row : values) {
                    for (Pair<Integer, Integer> fkColumnNumber : fkColumnNumbers) {
                        if (!row.get(fkColumnNumber.getKey()).equals(row.get(fkColumnNumber.getValue()))) {
                            skipFkSameTable = false;
                            break;
                        }
                    }
                }
            }
            if (skipFkSameTable) {
                continue;
            }
            final Map<Integer, Pair<List<String>, List<String>>> fkList =
                fkGroupByRefTable.computeIfAbsent(fkData.refTableName, k -> new LinkedHashMap<>());
            fkList.put(fkIndex, new Pair<>(fkData.refColumns, fkData.columns));
        }

        for (Map.Entry<String, Map<Integer, Pair<List<String>, List<String>>>> e : fkGroupByRefTable.entrySet()) {
            final String currentTableName = e.getKey();
            // currentFkCols store ref column names
            final List<List<String>> currentFkRefCols = new ArrayList<>();
            final List<List<String>> currentFkCols = new ArrayList<>();
            final List<List<ColumnMeta>> currentFkColMetas = new ArrayList<>();
            final List<Integer> fkIndexes = new ArrayList<>();

            for (Map.Entry<Integer, Pair<List<String>, List<String>>> entry : e.getValue().entrySet()) {
                fkIndexes.add(entry.getKey());
                currentFkRefCols.add(entry.getValue().getKey());
                currentFkCols.add(entry.getValue().getValue());
            }

            if (currentFkRefCols.size() == 0) {
                continue;
            }

            final TableMeta currentTableMeta = ec.getSchemaManager(schemaName).getTable(currentTableName);
            currentFkRefCols.forEach(cols -> {
                final List<ColumnMeta> columnMetaList = new ArrayList<>();
                for (String columnName : cols) {
                    columnMetaList.add(currentTableMeta.getColumnIgnoreCase(columnName));
                }
                currentFkColMetas.add(columnMetaList);
            });

            List<Set<String>> currentFkSets = new ArrayList<>();
            for (List<String> uk : currentFkRefCols) {
                final Set<String> ukSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                ukSet.addAll(uk);
                currentFkSets.add(ukSet);
            }

            List<String> currentFkNameList = new ArrayList<>();
            for (int i = 0; i < currentFkSets.size(); i++) {
                currentFkNameList.add(null);
            }

            // Rename column names from target columns to ref columns
            final List<String> mappedInsertColumns = new ArrayList<>();
            final List<String> mappedRefColumns = new ArrayList<>();
            Map<String, String> currentTableColMapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

            for (Integer fkIndex : fkIndexes) {
                Map<String, String> colMapping = fkTarColToRefColMapping.get(fkIndex);
                currentTableColMapping.putAll(colMapping);
            }

            List<List<Object>> mappedValues = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                mappedValues.add(new ArrayList<>());
            }

            List<Set<String>> refCols = new ArrayList<>();
            for (Map.Entry<String, String> entry : currentTableColMapping.entrySet()) {
                String tarCol = entry.getKey();
                String refCol = entry.getValue();
                final Set<String> refColSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                refColSet.add(refCol);
                refCols.add(refColSet);
                int colIndex = insertColumnIndex.get(tarCol);
                for (int i = 0; i < values.size(); i++) {
                    mappedValues.get(i).add(values.get(i).get(colIndex));
                }
                mappedInsertColumns.add(refCol.toUpperCase(Locale.ROOT));
                mappedRefColumns.add(tarCol.toUpperCase(Locale.ROOT));
            }

            List<String> refColumns = new ArrayList<>();
            tableMeta.getAllColumns().forEach(t -> refColumns.add(t.getName()));

            List<List<Object>> refValues = new ArrayList<>();
            for (List<Object> value : values) {
                List<Object> refValue = new ArrayList<>();
                for (int i = 0; i < value.size(); i++) {
                    String columnName = refColumns.get(i);
                    if (mappedRefColumns.stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
                        refValue.add(value.get(i));
                    }
                }
                refValues.add(refValue);
            }

            selects.addAll(buildSelects(insert, SqlSelect.LockMode.SHARED_LOCK, ec, currentTableName, new ArrayList<>(),
                mappedInsertColumns, currentFkCols, currentFkColMetas, currentFkNameList, totalFk, currentFkSets,
                values, true, false, false));

            totalFk += currentFkSets.size();
        }

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(ec);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();
        List<List<Object>> results;
        if (selects.size() != 0) {
            ExecutionContext selectEc = ec.copy();
            selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));
            results = executePhysicalPlan(selects, schemaName, selectEc, (rowCount) -> memoryAllocator
                .allocateReservedMemory(MemoryEstimator.calcSelectValuesMemCost(rowCount, selectRowType)));
        } else {
            results = Collections.emptyList();
        }

        final Map<Long, Set<Long>> resultsByFk = new HashMap<>();
        for (long i = 0; i < totalFk; i++) {
            resultsByFk.put(i, new HashSet<>());
        }

        // Check foreign key
        if (!results.isEmpty()) {
            // Group results by FK
            // Map[fkIndex, List[valueIndex]]
            for (List<Object> result : results) {
                final Long valueIndex = (Long) result.get(0);
                final Long fkIndex = (Long) result.get(1);
                resultsByFk.get(fkIndex).add(valueIndex);
            }
        }

        for (int i = 0; i < values.size(); i++) {
            for (Map.Entry<Long, Set<Long>> entry : resultsByFk.entrySet()) {
                final Set<Long> valueIndexes = entry.getValue();
                if (!valueIndexes.contains((long) i)) {
                    violatedValues.add(i);
                    break;
                }
            }
        }

        return violatedValues;
    }

    protected static List<List<Object>> getInputValues(LogicalDynamicValues input,
                                                       List<Map<Integer, ParameterContext>> currentBatchParameters,
                                                       ExecutionContext ec) {
        final List<List<Object>> values = new ArrayList<>();
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        currentBatchParameters.forEach(param -> values.add(RexUtils.buildRowValue(rexRow, null, param, ec)));
        return values;
    }

    protected RelDataType getRowTypeForColumns(LogicalInsert insert, Collection<ColumnMeta> columns) {
        final List<RelDataType> fieldTypes = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        columns.forEach(cm -> {
            fieldTypes.add(cm.getField().getRelType());
            fieldNames.add(cm.getName());
        });
        return insert.getCluster().getTypeFactory().createStructType(fieldTypes, fieldNames);
    }

    protected int insertSelectHandle(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                     HandlerParams handlerParams) {
        int affectRows;
        //在优化器OptimizeLogicalInsertRule进行了判断，选择执行模式
        if (logicalInsert.getInsertSelectMode() == LogicalInsert.InsertSelectMode.MPP) {
            final boolean useTrans = executionContext.getTransaction() instanceof IDistributedTransaction;
            if (useTrans) {
                //MPP暂不支持在事务下运行
                throw new TddlRuntimeException(ErrorCode.ERR_INSERT_SELECT,
                    "Insert Select isn't supported use MPP with transaction.");
            }
            LoggerFactory.getLogger(LogicalInsertHandler.class).info("Insert Select use MPP");
            affectRows = selectForInsertByMpp(logicalInsert, executionContext, handlerParams);
        } else {
            affectRows = selectForInsert(logicalInsert, executionContext, handlerParams);
        }

        return affectRows;
    }

    /**
     * In "insert ... select ..." case, select 100 values each time and insert
     * them. Or select all data at once.
     *
     * @return affectRows
     */
    protected int selectForInsert(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                  HandlerParams handlerParams) {
        RelNode input = logicalInsert.getInput();

        // Replace select clause with LogicalDynamicValues
        LogicalInsert newLogicalInsert = logicalInsert.buildInsertWithValues();

        // Select all data at once or streaming select for multiple times.
        boolean cacheAllOutput = executionContext.getTransaction() instanceof IDistributedTransaction;

        boolean insertSelectSelfByParallel =
            executionContext.getParamManager().getBoolean(ConnectionParams.INSERT_SELECT_SELF_BY_PARALLEL);
        //insert 和 select 操作同一个表时,非事务下可能会导致数据量 > 2倍，检测到自身表时，先select 再 insert (可hint绕过)
        if (!cacheAllOutput && !insertSelectSelfByParallel) {
            String insertTableName = logicalInsert.getLogicalTableName();
            final Set<String> selectTableNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            if (executionContext.getFinalPlan() != null
                && executionContext.getFinalPlan().getAst() instanceof SqlInsert) {
                SqlNode ast = executionContext.getFinalPlan().getAst();
                SqlNode selectSqlNode = ((SqlInsert) ast).getSource();

                ReplaceTableNameWithSomethingVisitor visitor =
                    new ReplaceTableNameWithSomethingVisitor(logicalInsert.getSchemaName(), executionContext) {
                        @Override
                        protected SqlNode buildSth(SqlNode sqlNode) {
                            if (sqlNode instanceof SqlIdentifier) {
                                selectTableNames.add(((SqlIdentifier) sqlNode).getLastName());
                            }
                            return sqlNode;
                        }
                    };
                selectSqlNode.accept(visitor);

                if (selectTableNames.contains(insertTableName)) {
                    cacheAllOutput = true;
                }
            }
        }
        boolean asyncCacheAllOutput = executionContext.isShareReadView() && executionContext.getParamManager()
            .getBoolean(ConnectionParams.MODIFY_WHILE_SELECT);
        if (asyncCacheAllOutput) {
            //todo: 目前无法读写连接同时存在，为后续实现读写并行作准备
            executionContext.setModifySelectParallel(true);
        }
        boolean canInsertByMulti = logicalInsert.getInsertSelectMode() == LogicalInsert.InsertSelectMode.MULTI;
        // How many records to insert each time in "insert ... select"
        long batchSize = executionContext.getParamManager().getLong(ConnectionParams.INSERT_SELECT_BATCH_SIZE);

        final long batchMemoryLimit =
            MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = batchMemoryLimit / batchSize;
        long maxMemoryLimit = executionContext.getParamManager().getLong(ConnectionParams.MODIFY_SELECT_BUFFER_SIZE);
        final long realMemoryPoolSize = Math.max(maxMemoryLimit, Math.max(batchMemoryLimit, BLOCK_SIZE));

        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, realMemoryPoolSize, MemoryType.OPERATOR);

        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        executionContext.setModifySelect(true);

        Cursor selectCursor = null;
        final ExecutionContext selectEc = executionContext.copy();

        try {
            selectCursor = ExecutorHelper.execute(input, selectEc, false, cacheAllOutput, asyncCacheAllOutput);

            int affectRows = 0;
            List<List<Object>> values = null;
            boolean firstBatch = true;

            ExecutionContext insertEc = executionContext.copy(new Parameters(executionContext.getParamMap()));

            // Update duplicate key update list if necessary
            final Map<Integer, Integer> duplicateKeyParamMapping = new HashMap<>();
            newLogicalInsert = newLogicalInsert
                .updateDuplicateKeyUpdateList(newLogicalInsert.getInsertRowType().getFieldCount(),
                    duplicateKeyParamMapping);
            // Select and insert loop
            do {
                values =
                    selectForModify(selectCursor, batchSize, memoryAllocator::allocateReservedMemory, memoryOfOneRow);
                if (values.isEmpty()) {
                    break;
                }

                //不是第一次执行（第一次单独执行，获取lastInsertId），values比较多，自适应转多线程doExecute
                if (canInsertByMulti && !firstBatch && values.size() >= batchSize) {
                    affectRows += doInsertSelectExecuteMulti(newLogicalInsert, insertEc, values, selectCursor,
                        batchSize, selectValuesPool, memoryAllocator, memoryOfOneRow, duplicateKeyParamMapping);
                    break;
                }

                // Construct params for each batch
                buildParamsForSelect(values, newLogicalInsert, duplicateKeyParamMapping, insertEc.getParams());
                HandlerParams newHandlerParams = new HandlerParams();
                affectRows += doExecute(newLogicalInsert, insertEc, newHandlerParams);

                // Only record the first id in all values.
                if (firstBatch) {
                    long lastInsertId = newHandlerParams.lastInsertId;
                    if (lastInsertId != 0) {
                        handlerParams.lastInsertId = lastInsertId;
                        handlerParams.returnedLastInsertId = lastInsertId;
                    }
                    firstBatch = false;
                }

                // Clear assigned sequence, otherwise it won't assign again.
                if (insertEc.getParams() != null) {
                    insertEc.getParams().getSequenceSize().set(0);
                    insertEc.getParams().getSequenceIndex().set(0);
                    insertEc.getParams().setSequenceBeginVal(null);
                }
                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            } while (true);

            return affectRows;
        } finally {
            if (selectCursor != null) {
                selectCursor.close(new ArrayList<>());
            }

            selectValuesPool.destroy();
        }
    }

    private int doInsertSelectExecuteMulti(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                           List<List<Object>> someValues, Cursor selectCursor,
                                           long batchSize, MemoryPool selectValuesPool,
                                           MemoryAllocatorCtx memoryAllocator,
                                           long memoryOfOneRow, Map<Integer, Integer> duplicateKeyParamMapping) {
        try {
            BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
            MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
            ParallelExecutor parallelExecutor = createInsertParallelExecutor(executionContext, logicalInsert,
                selectValues, duplicateKeyParamMapping, memoryControl);
            long valuesSize = memoryAllocator.getAllAllocated();
            parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());

            int affectRows =
                doParallelExecute(parallelExecutor, someValues, valuesSize, selectCursor, batchSize, memoryOfOneRow);

            executionContext.setPhySqlId(parallelExecutor.getPhySqlId().get());
            return affectRows;
        } catch (Throwable e) {
            if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_CRUCIAL_ERR_CHECK)
                || executionContext.isModifyBroadcastTable() || executionContext.isModifyGsiTable()) {
                // Can't commit
                executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL,
                    e.getMessage());
            }
            throw GeneralUtil.nestedException(e);
        }

    }

    /**
     * Set params by queried result, applying format like [[index1, index2],
     * [index1, index2]].
     *
     * @param values Queried result of select clause.
     * @param logicalInsert Constructed LogicalInsert with LogicalDynamicValues
     */
    public static void buildParamsForSelect(List<List<Object>> values, LogicalInsert logicalInsert,
                                            Map<Integer, Integer> duplicateKeyParamMapping,
                                            Parameters parameterSettings) {
        final RelNode input = logicalInsert.getInput();
        final Map<Integer, ParameterContext> currentParameter = parameterSettings.getCurrentParameter();
        final int fieldNum = input.getRowType().getFieldList().size();
        final int batchSize = values.size();
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(values.size());

        for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
            Map<Integer, ParameterContext> rowValues = new HashMap<>(fieldNum);
            List<Object> valueList = values.get(batchIndex);
            for (int fieldIndex = 0; fieldIndex < fieldNum; fieldIndex++) {
                Object value = null;
                // For manually added sequence column, fill the value with null.
                if (fieldIndex < valueList.size()) {
                    value = valueList.get(fieldIndex);
                }

                int newIndex = fieldIndex + 1;
                ParameterContext newPC = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    newIndex,
                    value instanceof EnumValue ? ((EnumValue) value).getValue() : value});

                rowValues.put(newIndex, newPC);
            }

            // Append parameters of duplicate key update
            if (GeneralUtil.isNotEmpty(duplicateKeyParamMapping)) {
                duplicateKeyParamMapping.forEach((k, v) -> {
                    final int oldIndex = k + 1;
                    final int newIndex = v + 1;
                    final ParameterContext oldPc = currentParameter.get(oldIndex);
                    final ParameterContext newPc = new ParameterContext(oldPc.getParameterMethod(), new Object[] {
                        newIndex, oldPc.getValue()});
                    rowValues.put(newIndex, newPc);
                });
            }

            batchParams.add(rowValues);
        }

        parameterSettings.setBatchParams(batchParams);
    }

    private int executeIndex(String tableName, SqlInsert sqlInsert, RelNode logicalInsert, List<RelNode> physicalPlan,
                             List<PhyTableInsertSharder.PhyTableShardResult> shardResults,
                             ExecutionContext executionContext, String schemaName) {
        InsertIndexExecutor executor =
            new InsertIndexExecutor((List<RelNode> inputs, ExecutionContext executionContext1) -> {
                QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext1);
                List<Cursor> inputCursors = new ArrayList<>(inputs.size());
                executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors,
                    schemaName);
                return inputCursors;
            },
                schemaName);

        try {
            return executor.execute(tableName, sqlInsert, logicalInsert, physicalPlan, shardResults, executionContext);
        } catch (Throwable e) {
            // Can't commit
            executionContext.getTransaction()
                .setCrucialError(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL, e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

    protected List<RelNode> replaceSeqAndBuildPhyPlan(LogicalInsert insert, ExecutionContext executionContext,
                                                      LogicalInsert.HandlerParams handlerParams) {
        List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>();
        PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
        List<RelNode> inputs = insert.getInput(insertPartitioner, shardResults, executionContext);
        handlerParams.usingSequence = insertPartitioner.isUsingSequence();
        handlerParams.lastInsertId = insertPartitioner.getLastInsertId();
        handlerParams.returnedLastInsertId = insertPartitioner.getReturnedLastInsertId();
        return inputs;
    }

    protected void handleException(ExecutionContext executionContext, Throwable e, boolean wrapDuplicate) {

        if (!executionContext.getParamManager().getBoolean(ConnectionParams.DML_SKIP_CRUCIAL_ERR_CHECK)
            || executionContext.isModifyBroadcastTable() || executionContext.isModifyGsiTable()) {
            // Can't commit
            executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL,
                e.getMessage());
        }

        if (e instanceof TddlNestableRuntimeException) {
            // If it's in INSERT IGNORE / INSERT ON DUPLICATE KEY UPDATE /
            // REPLACE, duplicate key error shouldn't happen. But we'are using
            // INSERT instead, which causing duplicate key happens. So it
            // happens, we should rephrase the error message to tell the user
            // that it doesn't support duplicate key in the statement.
            if (wrapDuplicate && GsiUtils
                .vendorErrorIs((TddlNestableRuntimeException) e, SQLSTATE_DUP_ENTRY, ER_DUP_ENTRY)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INSERT_DUPLICATE_VALUES, e);
            }
        }

        throw GeneralUtil.nestedException(e);
    }

    public static ParallelExecutor createInsertParallelExecutor(ExecutionContext ec, LogicalInsert logicalInsert,
                                                                BlockingQueue<List<List<Object>>> selectValues,
                                                                Map<Integer, Integer> duplicateKeyParamMapping,
                                                                MemoryControlByBlocked memoryControl) {
        String schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = ec.getSchemaName();
        }

        boolean useTrans = ec.getTransaction() instanceof IDistributedTransaction;
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        boolean isSingleTable = or.isTableInSingleDb(logicalInsert.getLogicalTableName());

        Set<String> allGroupNames = new HashSet<>();
        List<String> groups = ExecUtils.getTableGroupNames(schemaName, logicalInsert.getLogicalTableName(), ec);
        allGroupNames.addAll(groups);
        //包含gsi情况下，auto库可能主表和GSI的group不同
        List<String> gsiTables = GlobalIndexMeta.getIndex(logicalInsert.getLogicalTableName(), schemaName, ec)
            .stream().map(TableMeta::getTableName).collect(Collectors.toList());
        for (String gsi : gsiTables) {
            groups = ExecUtils.getTableGroupNames(schemaName, gsi, ec);
            allGroupNames.addAll(groups);
        }
        List<String> groupNames = Lists.newArrayList(allGroupNames);
        List<String> phyParallelSet = PhyTableOperationUtil.buildGroConnSetFromGroups(ec, groupNames);

        Pair<Integer, Integer> threads =
            ExecUtils.calculateLogicalAndPhysicalThread(ec, phyParallelSet.size(), isSingleTable, useTrans);
        int logicalThreads = threads.getKey();
        int physicalThreads = threads.getValue();

        LoggerFactory.getLogger(LogicalInsertHandler.class).info(
            "Insert select by ParallelExecutor, useTrans: " + useTrans + "; logicalThreads: " + logicalThreads
                + "; physicalThreads: " + physicalThreads);

        ParallelExecutor parallelExecutor = new ParallelExecutor(memoryControl);
        List<ExecuteJob> executeJobs = new ArrayList<>();

        for (int i = 0; i < logicalThreads; i++) {
            ExecuteJob executeJob =
                new InsertSelectExecuteJob(ec, parallelExecutor, logicalInsert, duplicateKeyParamMapping);
            executeJobs.add(executeJob);
        }

        parallelExecutor.createGroupRelQueue(ec, physicalThreads, phyParallelSet, !useTrans);
        parallelExecutor.setParam(selectValues, executeJobs);
        return parallelExecutor;
    }

    /**
     * Insert select 采用Mpp运行，充分利用CN资源
     */
    protected int selectForInsertByMpp(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                       HandlerParams handlerParams) {
        RelNode input = logicalInsert.getInput();
        LogicalInsert newLogicalInsert = logicalInsert;

        //如果select是简单的LogicalView，则直接和Insert合为一个Mpp的PlanFragment，减少values在CN间传递
        if (input instanceof Gather && ((Gather) input).getInput() instanceof LogicalView) {
            input = ((Gather) input).getInput();
            newLogicalInsert = (LogicalInsert) logicalInsert.copy(logicalInsert.getTraitSet(), ImmutableList.of(input));
        }
        //如果select较为复杂，如mergeSort等，增加Exchange，防止Insert的并行度为1
        if (!(newLogicalInsert.getInput() instanceof Exchange)
            && !(newLogicalInsert.getInput() instanceof LogicalView)) {
            MppExchange exchange = MppExchange.create(logicalInsert.getInput(), RelDistributions.RANDOM_DISTRIBUTED);
            newLogicalInsert =
                (LogicalInsert) newLogicalInsert.copy(newLogicalInsert.getTraitSet(), ImmutableList.of(exchange));
        }
        //防止因为WorkloadType导致Insert并行度为1
        executionContext.setWorkloadType(WorkloadType.AP);
        Cursor cursor = ExecutorHelper.executeCluster(newLogicalInsert, executionContext);

        return ExecUtils.getAffectRowsByCursor(cursor);
    }
}
