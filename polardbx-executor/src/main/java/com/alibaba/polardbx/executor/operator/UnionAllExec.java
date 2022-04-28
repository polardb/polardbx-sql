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
    import com.alibaba.polardbx.executor.chunk.Chunk;
    import com.alibaba.polardbx.executor.chunk.ChunkConverter;
    import com.alibaba.polardbx.executor.chunk.Converters;
    import com.alibaba.polardbx.optimizer.context.ExecutionContext;
    import com.alibaba.polardbx.optimizer.core.datatype.DataType;

    import java.util.ArrayList;
    import java.util.List;

/**
 * union all chunk executor
 */
public class UnionAllExec extends AbstractExecutor {

    private final List<Executor> inputs;

    private final List<DataType> outputColumnMeta;

    private final List<ChunkConverter> inputChunkGetters;

    private int currentExecIndex;
    private boolean isFinish;
    private ListenableFuture<?> blocked;

    public UnionAllExec(final List<Executor> inputs, List<DataType> outputColumnMeta, ExecutionContext context) {
        super(context);
        this.inputs = inputs;
        this.outputColumnMeta = outputColumnMeta;
        int[] columnIndexes = new int[outputColumnMeta.size()];
        DataType[] targetTypes = new DataType[outputColumnMeta.size()];
        for (int i = 0; i < outputColumnMeta.size(); i++) {
            columnIndexes[i] = i;
            targetTypes[i] = outputColumnMeta.get(i);
        }
        inputChunkGetters = new ArrayList<>();
        for (Executor input : inputs) {
            inputChunkGetters.add(Converters.createChunkConverter(
                input.getDataTypes(), columnIndexes, targetTypes, context));
        }
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        inputs.stream().forEach(e -> e.open());
    }

    @Override
    Chunk doNextChunk() {
        while (true) {
            if (currentExecIndex < inputs.size()) {
                Chunk input = inputs.get(currentExecIndex).nextChunk();
                if (input == null) {
                    if (inputs.get(currentExecIndex).produceIsFinished()) {
                        currentExecIndex++;
                        continue;
                    } else {
                        blocked = inputs.get(currentExecIndex).produceIsBlocked();
                    }
                }
                return input;
            } else {
                this.isFinish = true;
                return null;
            }
        }
    }

    @Override
    void doClose() {
        inputs.stream().forEach(e -> e.close());
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    @Override
    public List<Executor> getInputs() {
        return inputs;
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
