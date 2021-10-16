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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.InsertIndexExecutor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert.HandlerParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RowClassifier;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.ErrorCode.ER_DUP_ENTRY;
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
                affectRows = selectForInsert(logicalInsert, executionContext, handlerParams);
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
            logicalInsert.buildParamsForBatch(executionContext.getParams());
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
     * In broadcast tables and gsi tables, data must be consistent. That is,
     * some functions should be calculated in advance.
     */
    private boolean needConsistency(LogicalInsert logicalInsert, ExecutionContext ec) {
        String schemaName = logicalInsert.getSchemaName();
        String tableName = logicalInsert.getLogicalTableName();
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        return or.isBroadCast(tableName) || GlobalIndexMeta.hasIndex(tableName, schemaName, ec);
    }

    /**
     * Find those columns which must be literal. If it's a call, convert it to
     * literal.
     *
     * @return column indexes in the INSERT row type
     */
    private static List<Integer> getColumnsToReplaceCall(LogicalInsert logicalInsert, ExecutionContext ec) {
        Set<String> columnNames = new HashSet<>();
        String schemaName = logicalInsert.getSchemaName();
        String tableName = logicalInsert.getLogicalTableName();

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        // sharding keys of base table
        List<String> shardColumns = or.getSharedColumns(tableName);
        shardColumns.forEach(columnName -> columnNames.add(columnName.toUpperCase()));

        List<TableMeta> indexTableMetas = GlobalIndexMeta.getIndex(tableName, logicalInsert.getSchemaName(), ec);
        if (indexTableMetas != null && !indexTableMetas.isEmpty()) {
            // sharding keys of index tables
            for (TableMeta indexMeta : indexTableMetas) {
                shardColumns = or.getSharedColumns(indexMeta.getTableName());
                shardColumns.forEach(columnName -> columnNames.add(columnName.toUpperCase()));
            }

            // Even if it's normal INSERT, unique keys must be literals, so that
            // we can judge if it's null
            TableMeta baseTableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
            baseTableMeta.getUniqueIndexes(true).forEach(indexMeta -> indexMeta.getKeyColumns()
                .forEach(columnMeta -> columnNames.add(columnMeta.getName().toUpperCase())));
        }

        // column names to column indexes
        List<RelDataTypeField> fieldList = logicalInsert.getInput().getRowType().getFieldList();
        Set<Integer> calcColumnIndexes = new HashSet<>(columnNames.size());
        for (String name : columnNames) {
            for (int i = 0; i < fieldList.size(); i++) {
                if (fieldList.get(i).getName().equalsIgnoreCase(name)) {
                    calcColumnIndexes.add(i);
                }
            }
        }

        if (indexTableMetas != null && !indexTableMetas.isEmpty()) {
            // If VALUES(col) appears in the update list, col must be literal
            List<RexNode> updateList = logicalInsert.getDuplicateKeyUpdateList();
            if (updateList != null && !updateList.isEmpty()) {
                for (RexNode rexNode : updateList) {
                    getUpdateValuesColumns(rexNode, calcColumnIndexes);
                }
            }
        }

        return Lists.newArrayList(calcColumnIndexes);
    }

    private static void getUpdateValuesColumns(RexNode rexNode, Set<Integer> columnIndexes) {
        if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperator() == TddlOperatorTable.VALUES) {
                int columnIndex = ((RexInputRef) call.getOperands().get(0)).getIndex();
                columnIndexes.add(columnIndex);
                return;
            }

            List<RexNode> subNodes = call.getOperands();
            for (RexNode subNode : subNodes) {
                getUpdateValuesColumns(subNode, columnIndexes);
            }
        }
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

        if (null != logicalInsert.getPrimaryInsertWriter() && !logicalInsert.hasHint() && executionContext
            .getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE)) {

            RexUtils.updateParam(logicalInsert, executionContext, true, handlerParams);

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
                    executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL);
                }
                throw GeneralUtil.nestedException(e);
            }
        } else {
            executionContext.getExtraCmds().put(ConnectionProperties.GSI_CONCURRENT_WRITE, false);
        }

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
            executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 1);
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
        // How many records to insert each time in "insert ... select"
        long batchSize = executionContext.getParamManager().getLong(ConnectionParams.INSERT_SELECT_BATCH_SIZE);

        final long maxMemoryLimit =
            MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = maxMemoryLimit / batchSize;

        final String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        final MemoryPool selectValuesPool = executionContext.getMemoryPool().getOrCreatePool(
            poolName, Math.max(BLOCK_SIZE, maxMemoryLimit), MemoryType.OPERATOR);

        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        executionContext.setModifySelect(true);

        Cursor selectCursor = null;
        final ExecutionContext selectEc = executionContext.copy();

        try {
            selectCursor = ExecutorHelper.execute(input, selectEc, cacheAllOutput);

            int affectRows = 0;
            List<List<Object>> values = null;
            boolean firstBatch = true;

            ExecutionContext insertEc = executionContext.copy();

            // Update duplicate key update list if necessary
            final Map<Integer, Integer> duplicateKeyParamMapping = new HashMap<>();
            newLogicalInsert = newLogicalInsert
                .updateDuplicateKeyUpdateList(newLogicalInsert.getInsertRowType().getFieldCount(),
                    duplicateKeyParamMapping);
            // Select and insert loop
            do {
                values = selectValues(selectCursor, batchSize, memoryAllocator, memoryOfOneRow);
                if (values.isEmpty()) {
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

    /**
     * Set params by queried result, applying format like [[index1, index2],
     * [index1, index2]].
     *
     * @param values Queried result of select clause.
     * @param logicalInsert Constructed LogicalInsert with LogicalDynamicValues
     */
    private void buildParamsForSelect(List<List<Object>> values, LogicalInsert logicalInsert,
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

                // Convert inner type to JDBC types
                value = DataTypeUtil.toJavaObject(value);

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
                .setCrucialError(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL);
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
            executionContext.getTransaction().setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL);
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

    protected RowClassifier buildRowClassifier(final LogicalInsert insertOrReplace,
                                               final ExecutionContext ec) {
        return (writer, sourceRows, result) -> {
            if (null == result) {
                return result;
            }

            final LogicalDynamicValues input = RelUtils.getRelInput(insertOrReplace);

            final BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> identicalPartitionKeyChecker =
                getIdenticalPartitionKeyChecker(input, ec);

            // Classify insert/replace rows for UPDATE/REPLACE/DELETE/INSERT
            writer.classify(identicalPartitionKeyChecker, sourceRows, ec, result);

            return result;
        };
    }

    /**
     * Return a lambda expression for checking partition keys of two input row are identical
     */
    protected BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> getIdenticalPartitionKeyChecker(
        LogicalDynamicValues input,
        ExecutionContext executionContext) {
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);

        // Checker for identical partition key
        return (w, pair) -> {
            final RelocateWriter rw = w.unwrap(RelocateWriter.class);

            // Check partition key modified
            final List<Object> oldValue = pair.left;
            final Map<Integer, ParameterContext> newValue = pair.right;

            final Mapping skSourceMapping = rw.getIdentifierKeySourceMapping();
            final Object[] sourceSkValue = Mappings.permute(oldValue, skSourceMapping).toArray();

            final List<RexNode> targetSkRex = Mappings.permute(rexRow, skSourceMapping);
            final Object[] targetSkValue =
                targetSkRex.stream().map(rex -> RexUtils.getValueFromRexNode(rex, executionContext, newValue))
                    .toArray();

            final List<ColumnMeta> skMetas = rw.getIdentifierKeyMetas();
            final GroupKey sourceSkGk = new GroupKey(sourceSkValue, skMetas);
            final GroupKey targetSkGk = new GroupKey(targetSkValue, skMetas);

            // GroupKey(NULL).equals(GroupKey(NULL)) returns true
            return sourceSkGk.equals(targetSkGk);
        };
    }
}
