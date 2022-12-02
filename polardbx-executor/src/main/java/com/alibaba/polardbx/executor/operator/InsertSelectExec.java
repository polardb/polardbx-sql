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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
        final Map<Integer, Integer> duplicateKeyParamMapping = new HashMap<>();
        newInsert = newInsert
            .updateDuplicateKeyUpdateList(newInsert.getInsertRowType().getFieldCount(),
                duplicateKeyParamMapping);
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

    public void buildParamsFromInput(List<Row> rows, ExecutionContext context, Map<Integer, Integer> duplicateKeyParamMapping) {
        final int fieldNum = input.getDataTypes().size();
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(rows.size());
        final Map<Integer, ParameterContext> currentParameter = context.getParams().getCurrentParameter();

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
                value = DataTypeUtil.toJavaObject(value);

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

}
