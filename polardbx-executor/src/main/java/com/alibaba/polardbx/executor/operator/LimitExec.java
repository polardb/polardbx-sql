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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * Limit executor
 */
public class LimitExec extends AbstractExecutor {

    private final List<Executor> inputs;
    private final long limit;

    private long fetched;
    private long skipped;

    private int currentExecIndex = 0;
    private boolean finished;
    private ListenableFuture<?> blocked;

    public LimitExec(final List<Executor> inputs, long offset, long limit, ExecutionContext context) {
        super(context);
        this.inputs = inputs;
        this.limit = limit;

        this.skipped = offset;
        this.fetched = limit;

        this.blocked = NOT_BLOCKED;
    }

    @Override
    void doOpen() {
        if (limit > 0) {
            createBlockBuilders();
            for (Executor input : inputs) {
                input.open();
            }
        }
    }

    @Override
    Chunk doNextChunk() {
        if (fetched <= 0) {
            return null;
        }

        while (true) {
            Chunk input = null;
            if (currentExecIndex < inputs.size()) {
                input = inputs.get(currentExecIndex).nextChunk();
                if (input == null) {
                    if (inputs.get(currentExecIndex).produceIsFinished()) {
                        currentExecIndex++;
                        continue;
                    }
                    this.blocked = inputs.get(currentExecIndex).produceIsBlocked();
                }
            } else {
                this.finished = true;
            }

            if (input == null) {
                return null;
            }

            if (input.getPositionCount() <= skipped) {
                // Skip the whole chunk
                skipped -= input.getPositionCount();
            } else {
                // Chop the input chunk
                long size = (Math.min(input.getPositionCount() - skipped, fetched));
                if (size == input.getPositionCount()) {
                    // minor optimization
                    skipped = 0;
                    fetched -= size;
                    return input;
                } else {
                    for (int i = 0; i < input.getBlockCount(); i++) {
                        for (int j = 0; j < size; j++) {
                            input.getBlock(i).writePositionTo((int) (j + skipped), blockBuilders[i]);
                        }
                    }
                    skipped = 0;
                    fetched -= size;
                    return buildChunkAndReset();
                }
            }
        }
    }

    @Override
    void doClose() {
        if (limit > 0) {
            for (Executor input : inputs) {
                input.close();
            }
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return inputs.get(0).getDataTypes();
    }

    @Override
    public List<Executor> getInputs() {
        return inputs;
    }

    @Override
    public boolean produceIsFinished() {
        return fetched <= 0 || finished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
