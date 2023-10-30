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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * Recursive Executor
 *
 * @author fangwu
 */
public class RecursiveCTEAnchorExec extends AbstractExecutor {
    private final String dataKey;

    // Internal States
    private int currentChunkIndex;
    private final List<DataType> dataTypes;
    private boolean isFinished = false;

    private final ListenableFuture<?> blocked;

    public RecursiveCTEAnchorExec(String cteName,
                                  List<DataType> dataTypes,
                                  ExecutionContext context) {
        super(context);
        this.dataKey = RecursiveCTEExec.buildCTEKey(cteName);
        this.blocked = NOT_BLOCKED;
        this.dataTypes = dataTypes;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
    }

    @Override
    Chunk doNextChunk() {
        List<Chunk> chunks = (List<Chunk>) context.getCacheRefs().get(dataKey.hashCode());
        if (chunks == null) {
            throw GeneralUtil.nestedException(" recursive cte anchor data not ready" + dataKey);
        }
        if (currentChunkIndex >= chunks.size()) {
            isFinished = true;
            return null;
        } else {
            return chunks.get(currentChunkIndex++);
        }
    }

    @Override
    void doClose() {
        // data mem should be recycled by register
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
