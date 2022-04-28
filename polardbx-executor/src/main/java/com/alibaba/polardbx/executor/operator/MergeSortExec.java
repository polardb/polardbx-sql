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
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.util.ChunkWithPosition;
import com.alibaba.polardbx.executor.operator.util.ChunkWithPositionComparator;
import com.alibaba.polardbx.executor.operator.util.MergeSortedChunks;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Merge Sort Executor
 */
public class MergeSortExec extends AbstractExecutor {

    private final List<Executor> inputs;
    private final List<OrderByOption> orderBys;
    private final long limit;
    private long fetched;
    private long skipped;

    private WorkProcessor<Chunk> sortedPages;
    private boolean ignoreMergeSort;
    private boolean finished;
    private ListenableFuture<?> blocked;

    public MergeSortExec(List<Executor> inputs, List<OrderByOption> orderBys, long offset, long limit,
                         ExecutionContext context) {
        super(context);

        this.inputs = inputs;
        this.orderBys = orderBys;
        this.limit = limit;
        this.skipped = offset;
        this.fetched = limit;
        this.ignoreMergeSort = ((inputs.size() == 1) && (offset == 0) && limit == Long.MAX_VALUE);
        this.blocked = NOT_BLOCKED;
    }

    private WorkProcessor<Chunk> mergeSortProcessor(List<Executor> inputs) {
        List<WorkProcessor<Chunk>> sortedChunks = inputs.stream().map(
            input -> chunks(input)).collect(Collectors.toList());
        List<DataType> dataTypes = getDataTypes();
        ChunkWithPositionComparator comparator = new ChunkWithPositionComparator(orderBys, dataTypes);

        BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate =
            (chunkBuilder, ChunkWithPosition) -> chunkBuilder.isFull();
        return MergeSortedChunks.mergeSortedPages(
            sortedChunks, comparator, dataTypes, chunkLimit, chunkBreakPredicate, null, context);
    }

    public WorkProcessor<Chunk> chunks(Executor inputExecutor) {
        return WorkProcessor.create(() -> {
            Chunk page = inputExecutor.nextChunk();
            if (page == null) {
                if (inputExecutor.produceIsFinished()) {
                    return WorkProcessor.ProcessState.finished();
                }

                ListenableFuture<?> blocked = inputExecutor.produceIsBlocked();
                return WorkProcessor.ProcessState.blocked(blocked);
            }

            return WorkProcessor.ProcessState.ofResult(page);
        });
    }

    @Override
    void doOpen() {
        if (limit > 0) {
            createBlockBuilders();
            for (Executor input : inputs) {
                input.open();
            }
            if (!ignoreMergeSort) {
                this.sortedPages = mergeSortProcessor(inputs);
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
            if (ignoreMergeSort) {
                input = inputs.get(0).nextChunk();
                if (input == null) {
                    if (inputs.get(0).produceIsFinished()) {
                        this.finished = true;
                    }
                    this.blocked = inputs.get(0).produceIsBlocked();
                }
            } else {
                if (sortedPages == null || !sortedPages.process() || sortedPages.isFinished()) {
                    //ingore
                } else {
                    input = this.sortedPages.getResult();
                }
            }
            if (input == null) {
                return input;
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

    //MergeSort 是RootProducer，所以这里认为它的input是empty
    @Override
    public List<Executor> getInputs() {
        return inputs;
    }

    @Override
    public boolean produceIsFinished() {
        if (ignoreMergeSort) {
            return fetched <= 0 || finished;
        } else {
            return fetched <= 0 || (sortedPages != null && sortedPages.isFinished());
        }
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        if (ignoreMergeSort) {
            return blocked;
        } else {
            if (sortedPages.isBlocked()) {
                return sortedPages.getBlockedFuture();
            } else {
                return NOT_BLOCKED;
            }
        }
    }
}
