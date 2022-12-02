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

import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.operator.LocalBufferExec;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.alibaba.polardbx.common.properties.ConnectionParams.INSERT_SELECT_LIMIT;
import static com.alibaba.polardbx.common.properties.ConnectionParams.UPDATE_DELETE_SELECT_LIMIT;

public class CacheCursor implements Cursor {

    protected static final Logger log = LoggerFactory.getLogger(LocalBufferExec.class);

    protected final MemoryAllocatorCtx memoryAllocator;
    private SpillerFactory spillerFactory;
    private DataType[] dataTypes;

    private Cursor cursor;
    private final List<Row> bufferRows = new LinkedList<>();
    protected Spiller spiller;
    protected int chunkLimit;
    private BlockBuilder[] blockBuilders;
    private Iterator<Chunk> iterator;
    protected Chunk currentChunk;
    protected int currentPos;
    protected CursorMeta cursorMeta;
    private ListenableFuture<?> spillFuture;
    private MemoryPool pool;
    private long estimateRowSize;
    private boolean supportSpill;
    private List<Throwable> ex = new ArrayList<>();
    private SpillMonitor spillMonitor;
    private ExecutionContext context;

    public CacheCursor(
        ExecutionContext context, SpillerFactory spillerFactory, Cursor cursor, long estimateRowSize) {
        this.spillerFactory = spillerFactory;
        this.cursorMeta = CursorMeta.build(cursor.getReturnColumns());
        this.dataTypes = new DataType[cursor.getReturnColumns().size()];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = cursor.getReturnColumns().get(i).getDataType();
        }
        this.cursor = cursor;
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);

        String memoryName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        long totalEstimate = Math.max(context.getParamManager().getLong(INSERT_SELECT_LIMIT),
            context.getParamManager().getLong(UPDATE_DELETE_SELECT_LIMIT)) * estimateRowSize;
        this.pool = context.getMemoryPool().getOrCreatePool(
            memoryName,
            Math.max(context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE), totalEstimate),
            MemoryType.OPERATOR);
        this.memoryAllocator = pool.getMemoryAllocatorCtx();
        this.estimateRowSize = estimateRowSize;
        this.supportSpill = context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL_OUTPUT);
        this.spillMonitor = context.getQuerySpillSpaceMonitor();
        this.context = context;
    }

    public CacheCursor(SpillerFactory spillerFactory, Cursor cursor, MemoryPool pool, long estimateRowSize,
                       QuerySpillSpaceMonitor spillMonitor) {
        // TODO execution context can be removed from cache cursor and block builders
        ExecutionContext context = new ExecutionContext();
        this.spillerFactory = spillerFactory;

        this.cursorMeta = CursorMeta.build(cursor.getReturnColumns());
        this.dataTypes = new DataType[cursor.getReturnColumns().size()];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = cursor.getReturnColumns().get(i).getDataType();
        }
        this.cursor = cursor;
        this.estimateRowSize = estimateRowSize;
        this.chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        this.pool = pool;
        this.memoryAllocator = pool.getMemoryAllocatorCtx();
        this.supportSpill = true;
        this.spillMonitor = spillMonitor;
        this.context = context;
    }

    private final void createBlockBuilders() {
        if (blockBuilders == null) {
            // Create all block builders by default
            blockBuilders = new BlockBuilder[dataTypes.length];
            for (int i = 0; i < dataTypes.length; i++) {
                blockBuilders[i] = BlockBuilders.create(dataTypes[i], context);
            }
        }
    }

    final Chunk buildChunkAndReset() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    final int currentPosition() {
        return blockBuilders[0].getPositionCount();
    }

    @Override
    public Row next() {
        if (cursor != null) {
            cacheAllRows();
        }

        if (spiller != null) {
            while (true) {
                if (currentChunk == null) {
                    if (iterator.hasNext()) {
                        currentChunk = iterator.next();
                    }
                    if (currentChunk == null) {
                        return null;
                    }
                }

                if (currentPos < currentChunk.getPositionCount()) {
                    Row row = currentChunk.rowAt(currentPos++);
                    row.setCursorMeta(cursorMeta);
                    return row;
                } else {
                    currentChunk = null;
                    currentPos = 0;
                }
            }
        } else {
            if (!bufferRows.isEmpty()) {
                return bufferRows.remove(0);
            } else {
                return null;
            }
        }
    }

    public void cacheAllRows() {
        Row currentRow;
        while ((currentRow = cursor.next()) != null) {
            try {
                long rowSize = currentRow.estimateSize();
                if (rowSize <= 0) {
                    rowSize = estimateRowSize;
                }

                if (currentRow instanceof Chunk.ChunkRow) {
                    bufferRows.add(currentRow);
                } else {
                    bufferRows.add(new ArrayRow(currentRow.getValues().toArray(), rowSize));
                }
                memoryAllocator.allocateReservedMemory(rowSize);
            } catch (MemoryNotEnoughException t) {
                if (spillerFactory != null && supportSpill) {
                    if (spiller == null) {
                        createBlockBuilders();
                        spiller = spillerFactory.create(Lists.newArrayList(dataTypes), spillMonitor, null);
                    }
                    spill();
                } else {
                    throw t;
                }
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }

        if (spiller != null) {
            spill();
            iterator = getIteratorLater() ? null : getIterator();
        }
        cursor.close(ex);
        cursor = null;
    }

    protected Iterator<Chunk> getIterator() {
        return spiller.getSpills().get(0);
    }

    protected boolean getIteratorLater() {
        return false;
    }

    private void spill() {

        spillFuture = spiller.spill(new Iterator<Chunk>() {
            Iterator<Row> iterator = bufferRows.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Chunk next() {
                while (iterator.hasNext()) {
                    Row currentRow = iterator.next();
                    currentRow.setCursorMeta(cursorMeta);
                    for (int i = 0; i < currentRow.getColNum(); i++) {
                        blockBuilders[i].writeObject(dataTypes[i].convertFrom(currentRow.getObject(i)));
                    }
                    if (currentPosition() >= chunkLimit) {
                        return buildChunkAndReset();
                    }
                }

                if (blockBuilders[0].getPositionCount() > 0) {
                    return buildChunkAndReset();
                } else {
                    return null;
                }

            }
        }, true);

        if (spiller != null) {
            ExecUtils.checkException(spillFuture);
        }
        bufferRows.clear();
        memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);
    }

    @Override
    public List<Throwable> close(List<Throwable> exceptions) {
        exceptions.addAll(ex);
        try {
            if (spiller != null) {
                spiller.close();
            }
            if (spillFuture != null && !spillFuture.isDone()) {
                spillFuture.cancel(true);
            }
        } finally {
            if (cursor != null) {
                cursor.close(exceptions);
            }
            bufferRows.clear();
            if (pool != null) {
                pool.destroy();
            }
        }
        return exceptions;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() {
        return cursorMeta.getColumns();
    }
}
