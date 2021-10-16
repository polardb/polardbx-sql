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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;

import java.util.Iterator;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.alibaba.polardbx.executor.utils.ExecUtils.tryAndCheckException;

public class LocalAllBufferExec extends LocalBufferExec {

    protected static final Logger log = LoggerFactory.getLogger(LocalAllBufferExec.class);

    private final SpillerFactory spillerFactory;
    private Spiller spiller;
    protected Iterator<Chunk> iterator;

    private ListenableFuture<?> spillFuture;
    private boolean supportSpill;
    private SpillMonitor spillMonitor;
    private long usedBufferSize;

    public LocalAllBufferExec(
        ExecutionContext context,
        OutputBufferMemoryManager outputBufferMemoryManager, List<DataType> columnMetaList,
        SpillerFactory spillerFactory) {
        super(outputBufferMemoryManager, columnMetaList, false);
        Preconditions.checkNotNull(spillerFactory, "The spillerFactory is null!");
        this.spillerFactory = spillerFactory;
        this.notEmptyFuture = SettableFuture.create();
        this.supportSpill = context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL_OUTPUT);
        this.spillMonitor = context.getQuerySpillSpaceMonitor();
    }

    //--------------------- consume ---------------------

    @Override
    public void openConsume() {
    }

    @Override
    public void closeConsume(boolean force) {
        synchronized (lock) {
            if (closed) {
                return;
            }

            try {
                if (spiller != null) {
                    spiller.close();
                }
            } catch (Throwable t) {
                log.warn("close the spilled file!", t);
            }

            try {
                this.closed = true;
                if (usedBufferSize > 0) {
                    bufferMemoryManager.updateMemoryUsage(-usedBufferSize);
                }
                buffer.clear();
            } finally {
                this.notEmptyFuture.set(null);
            }
        }
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        synchronized (lock) {
            try {
                if (spiller != null) {
                    tryAndCheckException(spillFuture);
                }
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                buffer.put(chunk);
                usedBufferSize += chunk.getSizeInBytes();
                bufferMemoryManager.updateMemoryUsage(chunk.getSizeInBytes());
                if (bufferMemoryManager.isFull() && supportSpill) {
                    spill(false);
                } else if (bufferMemoryManager.isFull()) {
                    throw new TddlNestableRuntimeException("Memory not enough, and already use the memory size: " +
                        bufferMemoryManager.getBufferedBytes());
                }
            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }
    }

    @Override
    public void buildConsume() {
        // notify readers outside of lock since this may result in a callback
        synchronized (lock) {
            if (spiller != null) {
                spill(true);
            } else {
                this.notEmptyFuture.set(null);
            }
        }
    }

    private void spill(boolean build) {
        if (spillerFactory != null) {
            if (spiller == null) {
                spiller = spillerFactory.create(columnMetaList, spillMonitor, null);
            }
        }
        this.spillFuture = spiller.spill(buffer.iterator(), true);
        long usedSize = usedBufferSize;
        usedBufferSize = 0L;
        spillFuture.addListener(() -> {
            try {
                buffer.clear();
                bufferMemoryManager.updateMemoryUsage(-usedSize);
            } finally {
                if (build) {
                    iterator = spiller.getSpills().get(0);
                    notEmptyFuture.set(null);
                }
            }
        }, directExecutor());
    }

    @Override
    public boolean produceIsFinished() {
        synchronized (lock) {
            return (closed || notEmptyFuture.isDone());
        }
    }

    //--------------------- consume ---------------------

    //--------------------- produce-----------------
    @Override
    public Chunk nextChunk() {
        if (!notEmptyFuture.isDone() || closed) {
            return null;
        } else if (spiller == null) {
            Chunk ret = buffer.poll();
            if (ret != null) {
                usedBufferSize -= ret.getSizeInBytes();
                bufferMemoryManager.updateMemoryUsage(-ret.getSizeInBytes());
            }
            return ret;
        } else {
            tryAndCheckException(spillFuture);
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                return null;
            }
        }
    }

    @Override
    public Chunk takeChunk() {
        try {
            notEmptyFuture.get();
            return nextChunk();
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    //--------------------- produce-----------------

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        synchronized (lock) {
            return notEmptyFuture;
        }
    }

    @Override
    public void close() {
        closeConsume(true);
    }
}