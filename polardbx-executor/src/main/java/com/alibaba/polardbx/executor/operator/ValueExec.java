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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

public class ValueExec extends AbstractExecutor {

    private int count;

    private int currCount;

    private List<DataType> outputColumnMeta;

    public ValueExec(int count, List<DataType> outputColumnMeta, ExecutionContext context) {
        super(context);
        this.count = count;
        this.outputColumnMeta = outputColumnMeta;
        this.chunkLimit = Math.min(chunkLimit, count);
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        currCount = 0;
    }

    @Override
    Chunk doNextChunk() {
        if (currCount < count) {
            while (currentPosition() < chunkLimit && currCount < count) {
                currCount++;
                for (int i = 0; i < outputColumnMeta.size(); i++) {
                    blockBuilders[i].appendNull();
                }
            }
            return buildChunkAndReset();
        } else {
            return null;
        }
    }

    @Override
    void doClose() {
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public boolean produceIsFinished() {
        return currCount >= count;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
