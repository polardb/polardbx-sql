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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.DriverYieldSignal;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiPredicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.alibaba.polardbx.executor.mpp.operator.WorkProcessor.mergeSorted;
import static java.util.Objects.requireNonNull;

public class MergeSortedChunks {

    public static WorkProcessor<Chunk> mergeSortedPages(List<WorkProcessor<Chunk>> chunkProducers,
                                                        ChunkWithPositionComparator comparator,
                                                        List<DataType> types,
                                                        int chunkLimit,
                                                        BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate,
                                                        DriverYieldSignal yieldSignal,
                                                        ExecutionContext context) {
        requireNonNull(chunkProducers, "chunkProducers is null");
        requireNonNull(comparator, "comparator is null");
        requireNonNull(chunkBreakPredicate, "chunkBreakPredicate is null");

        List<WorkProcessor<ChunkWithPosition>> ChunkWithPositionProducers =
            chunkProducers.stream().map(pageProducer -> chunkWithPositions(pageProducer)).collect(toImmutableList());

        Comparator<ChunkWithPosition> chunkWithPositionComparator =
            (firstPageWithPosition, secondPageWithPosition) -> comparator
                .compareTo(firstPageWithPosition.getChunk(), firstPageWithPosition.getPosition(),
                    secondPageWithPosition.getChunk(), secondPageWithPosition.getPosition()
                );

        return buildPage(mergeSorted(ChunkWithPositionProducers, chunkWithPositionComparator), types, chunkLimit,
            chunkBreakPredicate, yieldSignal, context);
    }

    public static WorkProcessor<ChunkWithPosition> chunkWithPositions(WorkProcessor<Chunk> chunks) {
        return chunks.flatMap(chunk -> {
            return WorkProcessor.create(new WorkProcessor.Process<ChunkWithPosition>() {
                int position;

                @Override
                public WorkProcessor.ProcessState<ChunkWithPosition> process() {
                    if (position >= chunk.getPositionCount()) {
                        return WorkProcessor.ProcessState.finished();
                    }

                    return WorkProcessor.ProcessState.ofResult(new ChunkWithPosition(chunk, position++));
                }
            });
        });
    }

    public static WorkProcessor<Chunk> buildPage(WorkProcessor<ChunkWithPosition> chunkWithPositions,
                                                 List<DataType> types,
                                                 int chunkLimit,
                                                 BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate,
                                                 DriverYieldSignal yieldSignal, ExecutionContext context) {
        ChunkBuilder pageBuilder = new ChunkBuilder(types, chunkLimit, context);
        if (yieldSignal != null) {
            chunkWithPositions = chunkWithPositions.yielding(yieldSignal::isSet);
        }
        return chunkWithPositions.transform(chunkWithPositionOpt -> {
            boolean finished = !chunkWithPositionOpt.isPresent();
            if (finished && pageBuilder.isEmpty()) {
                return WorkProcessor.TransformationState.finished();
            }

            if (finished || chunkBreakPredicate.test(pageBuilder, chunkWithPositionOpt.get())) {
                Chunk page = pageBuilder.build();
                pageBuilder.reset();
                if (!finished) {
                    chunkWithPositionOpt.get().appendTo(pageBuilder);
                }
                return WorkProcessor.TransformationState.ofResult(page, !finished);
            }

            chunkWithPositionOpt.get().appendTo(pageBuilder);
            return WorkProcessor.TransformationState.needsMoreData();
        });
    }
}
