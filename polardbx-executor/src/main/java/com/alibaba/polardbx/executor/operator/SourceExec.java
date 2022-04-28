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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class SourceExec extends AbstractExecutor {

    //TODO 由于现在SourceExec本身没有太多复杂逻辑，所以inputData 和 outputData 现在是一样的
    private static final AtomicLongFieldUpdater<SourceExec> inputDataSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(SourceExec.class, "inputDataSizeLong");
    private static final AtomicLongFieldUpdater<SourceExec> inputPositionsUpdater =
        AtomicLongFieldUpdater.newUpdater(SourceExec.class, "inputPositionsLong");

    private volatile long inputDataSizeLong = 0L;
    private volatile long inputPositionsLong = 0L;

    public SourceExec(ExecutionContext context) {
        super(context);
    }

    public abstract void addSplit(Split split);

    public abstract void noMoreSplits();

    public abstract Integer getSourceId();

    abstract Chunk doSourceNextChunk();

    @Override
    public Chunk doNextChunk() {
        Chunk ret = doSourceNextChunk();
        if (ret != null) {
            inputDataSizeUpdater.addAndGet(this, ret.getSizeInBytes());
            inputPositionsUpdater.addAndGet(this, ret.getPositionCount());
        }
        return ret;
    }

    public long getInputDataSize() {
        return inputDataSizeUpdater.get(this);
    }

    public long getInputPositions() {
        return inputPositionsUpdater.get(this);
    }
}
