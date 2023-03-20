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

package com.alibaba.polardbx.executor.mpp.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LocalBufferExec implements Executor, ConsumerExecutor {

    protected static final Logger log = LoggerFactory.getLogger(LocalBufferExec.class);

    public static final Chunk END = new Chunk();

    public static final SettableFuture<?> NOT_EMPTY;

    static {
        NOT_EMPTY = SettableFuture.create();
        NOT_EMPTY.set(null);
    }

    protected BlockingQueue<Chunk> buffer = new LinkedBlockingQueue<>();
    protected final OutputBufferMemoryManager bufferMemoryManager;

    protected boolean closed = false;

    protected SettableFuture<?> notEmptyFuture = SettableFuture.create();

    protected boolean noData = false;
    protected final Object lock = new Object();
    protected final List<DataType> columnMetaList;
    protected final boolean syncMode;

    public LocalBufferExec(
        OutputBufferMemoryManager outputBufferMemoryManager, List<DataType> columnMetaList, boolean syncMode) {
        this.columnMetaList = columnMetaList;
        this.bufferMemoryManager = outputBufferMemoryManager;
        this.notEmptyFuture.set(null);
        this.syncMode = syncMode;
    }

    //--------------------- consume ---------------------

    @Override
    public void openConsume() {
    }

    @Override
    public void closeConsume(boolean force) {
        SettableFuture<?> notEmptyFuture;
        synchronized (lock) {
            this.putEnd();
            if (closed) {
                return;
            }
            closed = true;
            noData = true;
            long remainingSize = 0;
            for (Chunk chunk : buffer) {
                remainingSize += chunk.estimateSize();
            }
            bufferMemoryManager.updateMemoryUsage(-remainingSize);

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }
        notEmptyFuture.set(null);
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        SettableFuture<?> notEmptyFuture;
        synchronized (lock) {
            // ignore pages after finish
            if (!noData) {
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferMemoryManager.updateMemoryUsage(chunk.estimateSize());
                try {
                    buffer.put(chunk);
                } catch (Throwable t) {
                    throw new TddlNestableRuntimeException(t);
                }
            }
            // we just added a page (or we are finishing) so we are not empty
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }
        // notify readers outside of lock since this may result in a callback
        notEmptyFuture.set(null);
    }

    @Override
    public void buildConsume() {
        SettableFuture<?> notEmptyFuture;
        synchronized (lock) {
            if (noData) {
                this.putEnd();
                return;
            }
            noData = true;

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // notify readers outside of lock since this may result in a callback
        notEmptyFuture.set(null);

        this.putEnd();
    }

    private void putEnd() {
        if (syncMode) {
            try {
                buffer.put(END);
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }
    }

    @Override
    public boolean needsInput() {
        throw new UnsupportedOperationException("Invalid invoke!");
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        throw new UnsupportedOperationException("Invalid invoke!");
    }

    @Override
    public boolean produceIsFinished() {
        synchronized (lock) {
            return (closed || (noData && buffer.isEmpty()));
        }
    }

    //--------------------- consume ---------------------

    //--------------------- produce-----------------
    @Override
    public Chunk nextChunk() {
        if (closed || buffer.isEmpty()) {
            return null;
        } else {
            Chunk ret = buffer.poll();
            if (ret != null) {
                bufferMemoryManager.updateMemoryUsage(-ret.estimateSize());
            }
            return ret;
        }
    }

    public Chunk takeChunk() {
        try {
            Chunk ret = buffer.take();
            if (ret != null) {
                if (ret == END) {
                    return null;
                }
                bufferMemoryManager.updateMemoryUsage(-ret.estimateSize());
            }
            return ret;
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    //--------------------- produce-----------------

    @Override
    public void open() {

    }

    @Override
    public List<DataType> getDataTypes() {
        return columnMetaList;
    }

    @Override
    public void setId(int id) {

    }

    @Override
    public int getId() {
        return -1;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        synchronized (lock) {
            // if we need to block readers, and the current future is complete, create a new one
            if (!noData && buffer.isEmpty() && notEmptyFuture.isDone()) {
                notEmptyFuture = SettableFuture.create();
            }
            return notEmptyFuture;
        }
    }

    @Override
    public void close() {
        closeConsume(true);
    }

    @Override
    public void forceClose() {
        closeConsume(true);
    }
}