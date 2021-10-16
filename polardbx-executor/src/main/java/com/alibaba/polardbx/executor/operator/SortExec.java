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

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.ExternalSorter;
import com.alibaba.polardbx.executor.operator.util.MemSortor;
import com.alibaba.polardbx.executor.operator.util.Sorter;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;

import java.util.List;

public class SortExec extends AbstractExecutor implements ConsumerExecutor, MemoryRevoker {

    private final List<DataType> inputDataTypes;
    private final List<OrderByOption> orderBys;
    private SpillerFactory spillerFactory;
    private boolean spillEnabled;

    private MemoryPool memoryPool;
    private OperatorMemoryAllocatorCtx memoryAllocator;

    private Sorter sorter = null;

    private boolean finished = false;

    public SortExec(List<DataType> inputDataTypes, List<OrderByOption> orderBys, ExecutionContext context,
                    SpillerFactory spillerFactory) {
        super(context);
        this.inputDataTypes = inputDataTypes;
        this.orderBys = orderBys;
        this.spillerFactory = spillerFactory;
        this.spillEnabled = spillerFactory != null;

    }

    @Override
    void doOpen() {

    }

    @Override
    Chunk doNextChunk() {
        Chunk ret = this.sorter.nextChunk();
        if (ret == null) {
            this.finished = true;
        }
        return ret;
    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public void openConsume() {
        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        List<DataType> columns = getDataTypes();
        if (spillEnabled) {
            this.sorter = new ExternalSorter(
                memoryAllocator, orderBys, columns, chunkLimit, spillerFactory, context.getQuerySpillSpaceMonitor(),
                context);
        } else {
            this.sorter = new MemSortor(memoryAllocator, orderBys, columns, chunkLimit, false, context);
        }
    }

    @Override
    public void closeConsume(boolean force) {
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
        if (sorter != null) {
            this.sorter.close();
        }
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        this.sorter.addChunk(chunk);
    }

    @Override
    public void buildConsume() {
        if (sorter != null) {
            this.sorter.sort();
        }
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public List<DataType> getDataTypes() {
        return inputDataTypes;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        addSpillCnt(1);
        return sorter.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke() {
        sorter.finishMemoryRevoke();
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocator;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
