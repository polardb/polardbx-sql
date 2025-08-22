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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryControlByBlocked;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.repo.mysql.handler.LogicalInsertHandler;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

/**
 * @author lijiu.lzw
 */
public class InsertSelectExec extends SourceExec {
    private final Executor input;
    private final LogicalInsert insert;
    private final List<DataType> returnColumns;
    private ListenableFuture<?> listenableFuture;

    private boolean finished = false;
    private int affectRows = 0;

    public InsertSelectExec(LogicalInsert insert, Executor input, ExecutionContext context) {
        super(context);
        this.input = input;
        this.insert = insert;
        returnColumns = CalciteUtils.getTypes(insert.getRowType());
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        input.open();
        final Map mdcContext = MDC.getCopyOfContextMap();
        String schema = context.getSchemaName();
        String traceId = context.getTraceId();
        if (schema == null) {
            schema = insert.getSchemaName();
        }
        final String defaultSchema = schema;
        this.listenableFuture = context.getExecutorService().submitListenableFuture(schema, traceId, -1,
            () -> {
                DefaultSchema.setSchemaName(defaultSchema);
                MDC.setContextMap(mdcContext);
                insertExec();
                return null;
            },
            context.getRuntimeStatistics());
    }

    @Override
    public List<DataType> getDataTypes() {
        return returnColumns;
    }

    @Override
    void doClose() {
        forceClose();
    }

    @Override
    public synchronized void forceClose() {
        input.close();
        if (listenableFuture != null) {
            listenableFuture.cancel(true);
            listenableFuture = null;
        }
        finished = true;
    }

    @Override
    Chunk doSourceNextChunk() {
        //等插入完成，只返回一次affectRows，后续继续返回null
        if (!listenableFuture.isDone() || finished) {
            return null;
        }
        finished = true;
        //检查有没有出错
        try {
            listenableFuture.get();
        } catch (InterruptedException e) {
            GeneralUtil.nestedException("interrupted when doing insertSelect", e);
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }

        blockBuilders[0].writeObject(returnColumns.get(0).convertFrom(affectRows));

        return buildChunkAndReset();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return listenableFuture;
    }

    public void insertExec() {
        int chunkSize = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        ExecutionContext insertContext = context.copy();

        LogicalInsert newInsert = insert.buildInsertWithValues();

        final int valuesCount = newInsert.getInsertRowType().getFieldCount();

        // Update duplicate key update list if necessary
        final Map<Integer, Integer> duplicateKeyParamMapping = new HashMap<>();
        newInsert = newInsert.updateDuplicateKeyUpdateList(new AtomicInteger(valuesCount), duplicateKeyParamMapping);

        boolean use_parallel = context.getParamManager().getBoolean(ConnectionParams.INSERT_SELECT_MPP_BY_PARALLEL);
        //使用单机并行执行
        if (use_parallel) {
            affectRows += doInsertSelectExecuteMulti(newInsert, insertContext, duplicateKeyParamMapping);
            return;
        }

        final List<Row> bufferRows = new ArrayList<>(chunkSize);
        do {
            //获取select的数据,一个Chunk插入一次
            Chunk chunk;
            while ((chunk = input.nextChunk()) != null) {
                int rows = chunk.getPositionCount();
                for (int i = 0; i < rows; i++) {
                    bufferRows.add(chunk.rowAt(i));
                }
                if (bufferRows.isEmpty()) {
                    break;
                }

                //insert values
                buildParamsFromInput(bufferRows, insertContext, duplicateKeyParamMapping);
                Cursor cursor = ExecutorHelper.executeByCursor(newInsert, insertContext, false);

                affectRows += ExecUtils.getAffectRowsByCursor(cursor);

                bufferRows.clear();

                // Clear assigned sequence, otherwise it won't assign again.
                if (insertContext.getParams() != null) {
                    insertContext.getParams().getSequenceSize().set(0);
                    insertContext.getParams().getSequenceIndex().set(0);
                    insertContext.getParams().setSequenceBeginVal(null);
                }
            }

        } while (!input.produceIsFinished());

    }

    @Override
    public void addSplit(Split split) {
        throw new TddlNestableRuntimeException("Don't should be invoked!");
    }

    @Override
    public void noMoreSplits() {
        throw new TddlNestableRuntimeException("Don't should be invoked!");
    }

    @Override
    public Integer getSourceId() {
        return insert.getRelatedId();
    }

    public void buildParamsFromInput(List<Row> rows, ExecutionContext context,
                                     Map<Integer, Integer> duplicateKeyParamMapping) {
        final int fieldNum = input.getDataTypes().size();
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(rows.size());
        final Map<Integer, ParameterContext> currentParameter = context.getParams().getCurrentParameter();

        List<ColumnMeta> inputColumnMetas = CalciteUtils.buildColumnMeta(insert.getInsertRowType(), "InsertSelectExec");
        for (Row row : rows) {
            Map<Integer, ParameterContext> rowValues = new HashMap<>(fieldNum);
            List<Object> valueList = row.getValues();
            for (int fieldIndex = 0; fieldIndex < fieldNum; fieldIndex++) {
                Object value = null;
                // For manually added sequence column, fill the value with null.
                if (fieldIndex < valueList.size()) {
                    value = valueList.get(fieldIndex);
                }

                // Convert inner type to JDBC types
                value = DataTypeUtil.toJavaObject(inputColumnMetas.get(fieldIndex), value);

                int newIndex = fieldIndex + 1;
                ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    newIndex,
                    value instanceof EnumValue ? ((EnumValue) value).getValue() : value});

                rowValues.put(newIndex, newPc);
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

        context.getParams().setBatchParams(batchParams);
    }

    private int doInsertSelectExecuteMulti(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                           Map<Integer, Integer> duplicateKeyParamMapping) {
        long batchSize = executionContext.getParamManager().getLong(ConnectionParams.INSERT_SELECT_BATCH_SIZE);

        //暂时用insert的类型估算，input只有datatype
        final long batchMemoryLimit =
            MemoryEstimator.calcSelectValuesMemCost(batchSize, logicalInsert.getRowType());
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

        try {
            BlockingQueue<List<List<Object>>> selectValues = new LinkedBlockingQueue<>();
            MemoryControlByBlocked memoryControl = new MemoryControlByBlocked(selectValuesPool, memoryAllocator);
            ParallelExecutor parallelExecutor =
                LogicalInsertHandler.createInsertParallelExecutor(executionContext, logicalInsert,
                    selectValues, duplicateKeyParamMapping, memoryControl);
            parallelExecutor.getPhySqlId().set(executionContext.getPhySqlId());

            int affectRows = doParallelExecute(parallelExecutor, batchSize, memoryOfOneRow);

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

    public int doParallelExecute(ParallelExecutor parallelExecutor,
                                 long batchSize, long memoryOfOneRow) {
        if (parallelExecutor == null) {
            return 0;
        }
        int affectRows;
        try {
            parallelExecutor.start();

            // Select and DML loop
            do {
                AtomicLong batchRowSize = new AtomicLong(0L);
                List<List<Object>> values =
                    selectForModify(batchRowSize::getAndAdd, memoryOfOneRow);

                //input可能吐出null，但是还未结束，原因暂时未知
                if (values.isEmpty()) {
                    if (input.produceIsFinished()) {
                        parallelExecutor.finished();
                        break;
                    }
                    continue;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                parallelExecutor.getMemoryControl().allocate(batchRowSize.get());
                //将内存大小附带到List最后面，方便传送内存大小
                values.add(Collections.singletonList(batchRowSize.get()));
                parallelExecutor.getSelectValues().put(values);
            } while (true);

            //正常情况等待执行完
            affectRows = parallelExecutor.waitDone();

        } catch (Throwable t) {
            parallelExecutor.failed(t);

            throw GeneralUtil.nestedException(t);
        } finally {
            parallelExecutor.doClose();
        }
        return affectRows;
    }

    protected List<List<Object>> selectForModify(Consumer<Long> consumer, long memoryOfOneRow) {
        List<List<Object>> values = new ArrayList<>();
        Chunk chunk = input.nextChunk();
        if (chunk == null) {
            return values;
        }
        List<ColumnMeta> insertColumnMetas =
            CalciteUtils.buildColumnMeta(insert.getInsertRowType(), "InsertSelectExec");

        int rowNum = chunk.getPositionCount();
        for (int i = 0; i < rowNum; i++) {

            List<Object> valueList = chunk.rowAt(i).getValues();
            final List<Object> convertedColumnValues = new ArrayList<>();
            for (int j = 0; j < valueList.size(); j++) {
                convertedColumnValues.add(
                    DataTypeUtil.toJavaObject(
                        GeneralUtil.isNotEmpty(insertColumnMetas) ? insertColumnMetas.get(j) : null,
                        valueList.get(j)));
            }
            values.add(convertedColumnValues);
        }

        //计算内存大小
        try {
            consumer.accept(chunk.getElementUsedBytes());
        } catch (Exception e) {
            consumer.accept(memoryOfOneRow * values.size());
        }

        return values;
    }
}
