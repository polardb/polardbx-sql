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
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionParams.INSERT_SELECT_LIMIT;
import static com.alibaba.polardbx.common.properties.ConnectionParams.UPDATE_DELETE_SELECT_LIMIT;

/**
 * @author lijiu.lzw
 */
public class AsyncCacheCursor implements Cursor {
    protected static final Logger log = LoggerFactory.getLogger(AsyncCacheCursor.class);

    protected volatile boolean inited = false;

    protected final MemoryAllocatorCtx memoryAllocator;
    private final SpillerFactory spillerFactory;
    private final DataType[] dataTypes;

    private Cursor cursor;
    private final List<Row> bufferRows = new LinkedList<>();
    private Spiller spiller;
    protected int chunkLimit;
    private BlockBuilder[] blockBuilders;
    private Iterator<Chunk> iterator;
    private long canReadRows = 0;
    protected Chunk currentChunk;
    protected int currentPos;
    protected CursorMeta cursorMeta;
    private ListenableFuture<?> spillFuture;
    private final MemoryPool pool;
    private final long estimateRowSize;
    private final boolean supportSpill;
    private final List<Throwable> ex = new ArrayList<>();
    private final SpillMonitor spillMonitor;
    private final ExecutionContext context;

    private final Object lock = new Object();
    ListenableFuture<?> writeFuture = null;
    boolean writeFinished = false;
    private volatile Throwable throwable = null;
    long flushRowsNum = 0;

    public AsyncCacheCursor(
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
            Math.min(context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE), totalEstimate),
            MemoryType.OPERATOR);
        this.memoryAllocator = pool.getMemoryAllocatorCtx();
        this.estimateRowSize = estimateRowSize;
        this.supportSpill = context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL_OUTPUT);
        this.spillMonitor = context.getQuerySpillSpaceMonitor();
        this.context = context;
    }

    void doInit(){
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    startAsyncWrite();
                    inited = true;
                }
            }
        }

    }

    void startAsyncWrite(){
        final Map mdcContext = MDC.getCopyOfContextMap();

        writeFuture = context.getExecutorService().submitListenableFuture(
            context.getSchemaName(), context.getTraceId(), -1,
            () -> {
                MDC.setContextMap(mdcContext);
                try{
                    cacheAllRows();
                } catch (Throwable e){
                    if (throwable == null) {
                        throwable = e;
                    }
                    throw new TddlNestableRuntimeException(e);
                } finally {
                    writeFinish();
                }
                return null;
            }, context.getRuntimeStatistics());
    }

    void cacheAllRows(){
        Row currentRow;
        while ((currentRow = cursor.next()) != null && throwable == null) {
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
        if (throwable != null) {
            return;
        }

        if (spiller != null) {
            spill();
        } else {
            //说明内存足够存下
            writeProducer(bufferRows.size());
        }
        cursor.close(ex);
        cursor = null;
    }

    void writeProducer(long rows){
        synchronized (lock) {
            flushRowsNum += rows;
            lock.notifyAll();
        }
    }

    void writeFinish(){
        synchronized (lock) {
            writeFinished = true;
            lock.notifyAll();
        }
    }

    long readConsumer() {
        long canRead = 0;

        synchronized (lock) {
            while (throwable == null) {
                //写线程完成了，或者有可读的，直接返回
                if (writeFinished || flushRowsNum > 0) {
                    canRead = flushRowsNum;
                    flushRowsNum -= canRead;
                    break;
                }
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    throw new TddlNestableRuntimeException(e);
                }
            }
        }
        return canRead;
    }


    private void createBlockBuilders() {
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
        if (!inited) {
            doInit();
        }
        while (true) {
            if (canReadRows == 0) {
                canReadRows = readConsumer();
            }

            if (throwable != null) {
                throw new TddlNestableRuntimeException(throwable);
            }
            if (canReadRows == 0 && writeFinished) {
                //没有可读的了，并且写线程完成
                return null;
            }
            if (canReadRows > 0) {
                if (spiller != null) {
                    if (currentChunk == null) {
                        if (iterator == null) {
                            iterator = spiller.getSpills(canReadRows).get(0);
                        }
                        if (iterator.hasNext()) {
                            currentChunk = iterator.next();
                            currentPos = 0;
                        }
                        if (currentChunk == null) {
                            //本次canReadRows的iterator读完了,需要重新获取canReadRows
                            iterator = null;
                            canReadRows = 0;
                            continue;
                        }
                    }

                    //读当前currentChunk
                    if (currentPos < currentChunk.getPositionCount()) {
                        Row row = currentChunk.rowAt(currentPos++);
                        row.setCursorMeta(cursorMeta);
                        return row;
                    } else {
                        //currentChunk读完，重新获取currentChunk
                        currentChunk = null;
                        currentPos = 0;
                    }


                } else {
                    if (!bufferRows.isEmpty()) {
                        return bufferRows.remove(0);
                    } else {
                        canReadRows = 0;
                    }
                }
            }
        }
    }

    private void spill() {
        if (bufferRows.isEmpty()) {
            return;
        }

        spillFuture = spiller.spill(new Iterator<Chunk>() {
            final Iterator<Row> rowIterator = bufferRows.iterator();

            @Override
            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            @Override
            public Chunk next() {
                while (rowIterator.hasNext()) {
                    Row currentRow = rowIterator.next();
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
            spiller.flush();
            writeProducer(bufferRows.size());
        }

        bufferRows.clear();
        memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);
    }

    @Override
    public List<Throwable> close(List<Throwable> exceptions) {
        exceptions.addAll(ex);
        try {
            if (writeFuture != null && !writeFuture.isDone()) {
                writeFuture.cancel(true);
            }
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
