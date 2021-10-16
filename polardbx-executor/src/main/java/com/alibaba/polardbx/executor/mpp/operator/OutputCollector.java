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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class OutputCollector implements ConsumerExecutor {

    private static final AtomicLongFieldUpdater<OutputCollector> outputDataSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(OutputCollector.class, "outputDataSizeLong");
    private static final AtomicLongFieldUpdater<OutputCollector> outputPositionsUpdater =
        AtomicLongFieldUpdater.newUpdater(OutputCollector.class, "outputPositionsLong");

    private volatile long outputDataSizeLong = 0L;
    private volatile long outputPositionsLong = 0L;

    abstract void finish();

    abstract boolean isFinished();

    public void output(Chunk page) {
        if (page != null) {
            outputDataSizeUpdater.getAndAdd(this, page.getSizeInBytes());
            outputPositionsUpdater.getAndAdd(this, page.getPositionCount());
        }
        doOutput(page);
    }

    abstract void doOutput(Chunk page);

    @Override
    public void openConsume() {

    }

    @Override
    public void closeConsume(boolean force) {
        //TODO
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        this.output(chunk);
    }

    @Override
    public void buildConsume() {
        this.finish();
    }

    public long getOutputDataSize() {
        return outputDataSizeUpdater.get(this);
    }

    public long getOutputPositions() {
        return outputPositionsUpdater.get(this);
    }
}
