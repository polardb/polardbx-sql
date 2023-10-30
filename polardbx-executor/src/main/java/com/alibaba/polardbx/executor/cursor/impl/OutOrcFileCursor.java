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

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.operator.spill.OrcWriter;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.sql.OutFileParams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OutOrcFileCursor extends AbstractCursor {
    protected static final Logger log = LoggerFactory.getLogger(OutOrcFileCursor.class);
    protected DataType[] dataTypes;
    private Cursor cursor;
    private BlockBuilder[] blockBuilders;
    protected CursorMeta cursorMeta;
    protected ExecutionContext context;
    protected int affectRow = 0;
    protected MemoryPool pool;
    protected MemoryAllocatorCtx memoryAllocator;
    protected OrcWriter orcWriter;
    protected ListenableFuture<?> orcWriterFuture;

    public OutOrcFileCursor(
        ExecutionContext context, Cursor cursor, OutFileParams outFileParams) {
        super(false);
        this.cursorMeta = CursorMeta.build(cursor.getReturnColumns());
        this.dataTypes = new DataType[cursor.getReturnColumns().size()];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = cursor.getReturnColumns().get(i).getDataType();
        }
        this.cursor = cursor;
        outFileParams.setColumnMeata(cursor.getReturnColumns());
        this.context = context;
        createBlockBuilders();

        orcWriter = new OrcWriter(context, outFileParams, dataTypes);
        String memoryName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        this.pool = context.getMemoryPool().getOrCreatePool(
            memoryName,
            context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE),
            MemoryType.OPERATOR);
        memoryAllocator = pool.getMemoryAllocatorCtx();
    }

    protected final void createBlockBuilders() {
        if (blockBuilders == null) {
            blockBuilders = new BlockBuilder[dataTypes.length];
            for (int i = 0; i < dataTypes.length; i++) {
                blockBuilders[i] = BlockBuilders.create(dataTypes[i], context);
            }
        }
    }

    protected final void getOrcWriterFuture() {
        if (orcWriterFuture != null) {
            try {
                orcWriterFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected Row doNext() {
        Row currentRow;
        if (null == cursor || (currentRow = cursor.next()) == null) {
            return null;
        }

        List<Row> bufferRows = new ArrayList<>();
        while (currentRow != null) {
            try {
                long rowSize = currentRow.estimateSize();

                if (currentRow instanceof Chunk.ChunkRow) {
                    bufferRows.add(currentRow);
                    memoryAllocator.allocateReservedMemory(rowSize);
                } else {
                    bufferRows.add(new ArrayRow(cursorMeta, currentRow.getValues().toArray(), rowSize));
                    memoryAllocator.allocateReservedMemory(rowSize);
                }

            } catch (MemoryNotEnoughException t) {
                affectRow += bufferRows.size();
                orcWriter.writeRows(bufferRows);
                // getOrcWriterFuture();
                bufferRows = new ArrayList<>();
                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }

            currentRow = cursor.next();
        }

        if (bufferRows.size() > 0) {
            affectRow += bufferRows.size();
            orcWriter.writeRows(bufferRows);
        }

        orcWriter.writeRowsFinish();

        orcWriter.uploadToOss();

        memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);

        if (orcWriter != null) {
            orcWriter.close(false);
        }

        ArrayRow arrayRow = new ArrayRow(1, cursorMeta);
        arrayRow.setObject(0, affectRow);
        arrayRow.setCursorMeta(cursorMeta);
        return arrayRow;
    }

    @Override
    protected List<Throwable> doClose(List<Throwable> exceptions) {
        boolean needClearFile = exceptions.size() > 0;
        try {
            if (orcWriterFuture != null && !orcWriterFuture.isDone()) {
                orcWriterFuture.cancel(true);
                needClearFile = true;
            }
        } catch (Throwable t) {
            exceptions.add(t);
        }
        try {
            orcWriter.close(needClearFile);
        } catch (Throwable t) {
            exceptions.add(t);
        } finally {
            if (cursor != null) {
                exceptions = cursor.close(exceptions);
            }
        }
        return exceptions;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() {
        return cursorMeta.getColumns();
    }
}