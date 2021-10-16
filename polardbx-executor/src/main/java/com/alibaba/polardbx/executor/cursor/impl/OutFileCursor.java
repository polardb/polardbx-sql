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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilders;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleBufferSpiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import org.apache.calcite.sql.OutFileParams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OutFileCursor extends AbstractCursor {

    protected static final Logger log = LoggerFactory.getLogger(OutFileCursor.class);

    private DataType[] dataTypes;

    private Cursor cursor;
    private BlockBuilder[] blockBuilders;
    protected CursorMeta cursorMeta;
    private ListenableFuture<?> spillFuture;
    private ExecutionContext context;
    private AsyncFileSingleBufferSpiller singleStreamSpiller;
    private int affectRow = 0;
    private MemoryPool pool;
    protected final MemoryAllocatorCtx memoryAllocator;

    public OutFileCursor(
        ExecutionContext context, SpillerFactory spillerFactory, Cursor cursor, OutFileParams outFileParams) {
        super(false);
        this.cursorMeta = CursorMeta.build(cursor.getReturnColumns());
        this.dataTypes = new DataType[cursor.getReturnColumns().size()];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = cursor.getReturnColumns().get(i).getDataType();
        }
        this.cursor = cursor;
        outFileParams.setColumnMeata(cursor.getReturnColumns());
        SpillMonitor spillMonitor = context.getQuerySpillSpaceMonitor();
        this.context = context;
        createBlockBuilders();
        singleStreamSpiller = (AsyncFileSingleBufferSpiller)
            spillerFactory.getStreamSpillerFactory().create(
                spillMonitor.tag(), Lists.newArrayList(dataTypes), spillMonitor.newLocalSpillMonitor(), outFileParams);
        String memoryName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        this.pool = context.getMemoryPool().getOrCreatePool(
            memoryName,
            context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE),
            MemoryType.OPERATOR);
        memoryAllocator = pool.getMemoryAllocatorCtx();
    }

    private final void createBlockBuilders() {
        if (blockBuilders == null) {
            blockBuilders = new BlockBuilder[dataTypes.length];
            for (int i = 0; i < dataTypes.length; i++) {
                blockBuilders[i] = BlockBuilders.create(dataTypes[i], context);
            }
        }
    }

    private final void getSpillFuture() {
        if (spillFuture != null) {
            try {
                spillFuture.get();
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
                    bufferRows.add(new ArrayRow(currentRow.getValues().toArray(), rowSize));
                    memoryAllocator.allocateReservedMemory(rowSize);
                }

            } catch (MemoryNotEnoughException t) {
                affectRow += bufferRows.size();
                spillFuture =
                    singleStreamSpiller.spillRows(bufferRows.iterator(), memoryAllocator.getReservedAllocated());
                getSpillFuture();
                bufferRows = new ArrayList<>();
                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }

            currentRow = cursor.next();
        }

        if (bufferRows.size() > 0) {
            affectRow += bufferRows.size();
            spillFuture = singleStreamSpiller.spillRows(bufferRows.iterator(), memoryAllocator.getReservedAllocated());
        }

        getSpillFuture();

        memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);

        if (singleStreamSpiller != null) {
            singleStreamSpiller.closeWriter(false);
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
            if (spillFuture != null && !spillFuture.isDone()) {
                spillFuture.cancel(true);
                needClearFile = true;
            }
        } catch (Throwable t) {
            exceptions.add(t);
        }
        try {
            if (needClearFile) {
                singleStreamSpiller.close();
            } else {
                singleStreamSpiller.closeWriter(false);
            }
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