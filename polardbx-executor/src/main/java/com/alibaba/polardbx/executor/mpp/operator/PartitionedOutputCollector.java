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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.ClientBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputCollector extends OutputCollector {

    private final PagePartitioner partitionPartitioner;
    private boolean finished;
    private ListenableFuture<?> blocked = NOT_BLOCKED;

    public PartitionedOutputCollector(
        int partitionCount,
        List<Integer> prunePartitions,
        int fullPartCount,
        List<DataType> sourceTypes,
        boolean remotePairWise,
        List<DataType> outputType,
        List<Integer> partitionChannels,
        OutputBuffer outputBuffer,
        PagesSerdeFactory serdeFactory,
        int chunkLimit, ExecutionContext context) {
        this.partitionPartitioner = new PagePartitioner(
            partitionCount,
            prunePartitions,
            fullPartCount,
            partitionChannels,
            outputBuffer,
            serdeFactory,
            sourceTypes,
            remotePairWise,
            outputType,
            chunkLimit,
            context);
    }

    @Override
    public void openConsume() {
        this.partitionPartitioner.init();
    }

    @Override
    public void finish() {
        finished = true;
        blocked = partitionPartitioner.flush(true);
    }

    @Override
    public boolean isFinished() {
        return finished && consumeIsBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public void doOutput(Chunk page) {
        requireNonNull(page, "page is null");
        checkState(consumeIsBlocked().isDone(), "output is already blocked");

        if (page.getPositionCount() == 0) {
            return;
        }

        blocked = partitionPartitioner.partitionPage(page);
    }

    @Override
    public boolean needsInput() {
        return !finished && consumeIsBlocked().isDone();
    }

    private static class PagePartitioner {
        protected final OutputBuffer outputBuffer;
        protected final List<DataType> outputType;
        protected final RemotePartitionFunction partitionFunction;
        protected final List<Integer> partitionChannels;  //shuffle字段下标
        protected final PagesSerde serde;

        private ChunkConverter converter;

        private final int chunkLimit;
        private final ExecutionContext context;
        private final int partitionCount;

        //防止内存膨胀的一种优化策略
        private List<Chunk>[] chunkArraylist;
        protected List<ChunkBuilder> pageBuilders;

        /**
         * There are int arrays with size = chunk_limit for each parallelism.
         */
        private final int[][] partitionSelections;
        private final int[] selSizes;
        private final int[] partitionPositions;

        public PagePartitioner(
            int partitionCount,
            List<Integer> prunePartitions,
            int fullPartCount,
            List<Integer> partitionChannels,
            OutputBuffer outputBuffer,
            PagesSerdeFactory serdeFactory,
            List<DataType> sourceTypes,
            boolean remotePairWise,
            List<DataType> outputType,
            int chunkLimit,
            ExecutionContext context) {
            this.converter = Converters.createChunkConverter(sourceTypes, outputType, context);
            if (partitionCount == 1) {
                this.partitionFunction = new SinglePartitionFunction();
            } else if (partitionChannels.size() > 0) {
                if (remotePairWise) {
                    if (prunePartitions == null || prunePartitions.isEmpty()) {
                        boolean enableCompatible =
                            context.getParamManager().getBoolean(ConnectionParams.ENABLE_PAIRWISE_SHUFFLE_COMPATIBLE);
                        this.partitionFunction =
                            new PairWisePartitionFunction(partitionCount, fullPartCount, partitionChannels,
                                enableCompatible);
                    } else {
                        boolean enableCompatible =
                            context.getParamManager().getBoolean(ConnectionParams.ENABLE_PAIRWISE_SHUFFLE_COMPATIBLE);
                        this.partitionFunction =
                            new PrunedPairWisePartitionFunction(partitionCount, fullPartCount,
                                partitionChannels, new HashSet<>(prunePartitions), enableCompatible);
                    }
                } else {
                    this.partitionFunction =
                        new HashPartitionFunction(partitionCount, partitionChannels);
                }
            } else {
                this.partitionFunction = new RandomPartitionFunction(partitionCount);
            }
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");

            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.outputType = requireNonNull(outputType, "sourceTypes is null");
            this.serde = requireNonNull(serdeFactory, "serdeFactory is null")
                .createPagesSerde(outputType, context);
            this.chunkLimit = chunkLimit;
            this.context = context;
            this.partitionCount = partitionCount;

            this.partitionSelections = new int[partitionCount][chunkLimit];
            this.selSizes = new int[partitionCount];
            this.partitionPositions = new int[chunkLimit];
        }

        public void init() {
            synchronized (this) {
                if (chunkArraylist == null) {
                    ImmutableList.Builder<ChunkBuilder> pageBuilders = ImmutableList.builder();
                    for (int i = 0; i < partitionFunction.getPartitionCount(); i++) {
                        pageBuilders.add(new ChunkBuilder(outputType, chunkLimit, context));
                    }
                    this.pageBuilders = pageBuilders.build();

                    this.chunkArraylist = new List[partitionCount];
                    for (int i = 0; i < partitionCount; i++) {
                        this.chunkArraylist[i] = new ArrayList<>();
                    }
                }
            }
        }

        public ListenableFuture<?> partitionPage(Chunk chunk) {
            requireNonNull(chunk);

            boolean sendChunkArraylist = false;
            Chunk convertPage = converter.apply(chunk);

            // clear selSize
            Arrays.fill(selSizes, 0);

            // collect partition -> selSize -> positions[]
            for (int position = 0; position < convertPage.getPositionCount(); position++) {
                int partition = partitionFunction.getPartition(chunk, position);
                // partition == -1, means this function is pruned pairwise partition function
                // and this record can be discard
                if (partition == -1) {
                    continue;
                }
                partitionSelections[partition][selSizes[partition]] = position;
                selSizes[partition] = selSizes[partition] + 1;
            }

            for (int partition = 0; partition < selSizes.length; partition++) {
                final int partitionSize = selSizes[partition];
                int[] partitionSelection = partitionSelections[partition];
                ChunkBuilder pageBuilder = pageBuilders.get(partition);

                final int writablePositions = chunkLimit - pageBuilder.getDeclarePosition();
                if (partitionSize > writablePositions) {
                    // declarePosition -> chunkLimit -> declarePosition + partitionSize

                    pageBuilder.updateDeclarePosition(writablePositions);
                    for (int channel = 0; channel < outputType.size(); channel++) {
                        pageBuilder.appendTo(convertPage.getBlock(channel), channel, partitionSelection, 0,
                            writablePositions);
                    }

                    // check full
                    if (pageBuilder.isFull()) {
                        sendChunkArraylist = true;
                        Chunk pageBucket = pageBuilder.build();
                        this.chunkArraylist[partition].add(pageBucket);
                        pageBuilder.reset();
                    }

                    pageBuilder.updateDeclarePosition(partitionSize - writablePositions);
                    for (int channel = 0; channel < outputType.size(); channel++) {
                        pageBuilder.appendTo(convertPage.getBlock(channel), channel, partitionSelection,
                            writablePositions, partitionSize - writablePositions);
                    }

                } else {

                    // declarePosition -> declarePosition + partitionSize -> chunkLimit
                    pageBuilder.updateDeclarePosition(partitionSize);
                    for (int channel = 0; channel < outputType.size(); channel++) {
                        pageBuilder.appendTo(convertPage.getBlock(channel), channel, partitionSelection, 0,
                            partitionSize);
                    }
                }

                // check full
                if (pageBuilder.isFull()) {
                    sendChunkArraylist = true;
                    Chunk pageBucket = pageBuilder.build();
                    this.chunkArraylist[partition].add(pageBucket);
                    pageBuilder.reset();
                }
            }

            if (sendChunkArraylist) {
                return flush(chunkArraylist);
            } else {
                return flush(false);
            }
        }

        public ListenableFuture<?> flush(List<Chunk>[] bufferedPages) {
            List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
            for (int partition = 0; partition < bufferedPages.length; ++partition) {
                List<Chunk> bufferedPage = bufferedPages[partition];
                if (bufferedPage == null) {
                    continue;
                }
                ImmutableList.Builder<SerializedChunk> pages = new ImmutableList.Builder<>();

                for (int ind = 0; ind < bufferedPage.size(); ++ind) {
                    Chunk page = bufferedPage.get(ind);
                    //TODO 如果发送的chunk的函数虽然很小，但是数据本身很大，则这里最好考虑将做切分发送，既split into chunks.
                    ClientBuffer buffer = outputBuffer.getClientBuffer(partition);
                    SerializedChunk serializedPage = serde.serialize(
                        buffer != null && buffer.isPreferLocal(), page);
                    pages.add(serializedPage);
                }
                blockedFutures.add(outputBuffer.enqueue(partition, pages.build()));
                bufferedPage.clear();
            }
            ListenableFuture<?> future = Futures.allAsList(blockedFutures);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }

        public ListenableFuture<?> flush(boolean force) {
            if (pageBuilders == null) {
                return NOT_BLOCKED;
            }
            // add all full pages to output buffer
            List<ListenableFuture<?>> blockedFutures = new ArrayList<>();

            for (int partition = 0; partition < pageBuilders.size(); partition++) {
                ChunkBuilder partitionPageBuilder = pageBuilders.get(partition);
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Chunk pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();
                    //TODO 如果发送的chunk的函数虽然很小，但是数据本身很大，则这里最好考虑将做切分发送，既split into chunks.
                    ClientBuffer buffer = outputBuffer.getClientBuffer(partition);
                    SerializedChunk
                        serializedPages = serde.serialize(
                        buffer != null && buffer.isPreferLocal(), pagePartition);

                    blockedFutures.add(outputBuffer.enqueue(partition, Lists.newArrayList(serializedPages)));
                }
            }
            ListenableFuture<?> future = Futures.allAsList(blockedFutures);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }
    }

    public static class HashPartitionFunction implements RemotePartitionFunction {

        protected final int partitionCount;
        protected final int[] partitionChannelArray;
        protected final boolean isPowerOfTwo;

        public HashPartitionFunction(int partitionCount, List<Integer> partitionChannels) {
            this.partitionCount = partitionCount;
            this.partitionChannelArray = new int[partitionChannels.size()];
            for (int i = 0; i < partitionChannels.size(); i++) {
                partitionChannelArray[i] = partitionChannels.get(i);
            }

            this.isPowerOfTwo = MathUtils.isPowerOfTwo(partitionCount);
        }

        @Override
        public int getPartitionCount() {
            return partitionCount;
        }

        @Override
        public int getPartition(Chunk page, int position) {
            long hashCode = 0;
            for (int i = 0; i < partitionChannelArray.length; i++) {
                hashCode = hashCode * 31 + page.getBlock(partitionChannelArray[i]).hashCodeUseXxhash(position);
            }

            int partition = ExecUtils.directPartition(hashCode, partitionCount, isPowerOfTwo);
            checkState(partition >= 0 && partition < partitionCount);
            return partition;
        }
    }

    public static class PairWisePartitionFunction extends HashPartitionFunction {

        protected final int fullPartCount;

        protected final boolean isFullPartPowerOfTwo;

        protected final boolean enableCompatible;

        public PairWisePartitionFunction(int partitionCount, int fullPartCount, List<Integer> partitionChannels,
                                         boolean enableCompatible) {
            super(partitionCount, partitionChannels);
            this.fullPartCount = fullPartCount;
            this.isFullPartPowerOfTwo = MathUtils.isPowerOfTwo(fullPartCount);
            this.enableCompatible = enableCompatible;
        }

        @Override
        public int getPartition(Chunk page, int position) {
            long hashCode = 0;
            for (int i = 0; i < partitionChannelArray.length; i++) {
                hashCode = hashCode * 31 + page.getBlock(partitionChannelArray[i])
                    .hashCodeUnderPairWise(position, enableCompatible);
            }

            int partition =
                ExecUtils.partitionUnderPairWise(hashCode, partitionCount, fullPartCount, isFullPartPowerOfTwo);
            checkState(partition >= 0 && partition < partitionCount);
            return partition;
        }
    }

    public static class PrunedPairWisePartitionFunction extends PairWisePartitionFunction {
        private final Set<Integer> prunePartitions;

        public PrunedPairWisePartitionFunction(int partitionCount, int fullPartCount, List<Integer> partitionChannels,
                                               Set<Integer> prunePartitions, boolean enableCompatible) {
            super(partitionCount, fullPartCount, partitionChannels, enableCompatible);
            this.prunePartitions = prunePartitions;
        }

        @Override
        public int getPartition(Chunk page, int position) {
            long hashCode = 0;
            for (int i = 0; i < partitionChannelArray.length; i++) {
                hashCode = hashCode * 31 + page.getBlock(partitionChannelArray[i])
                    .hashCodeUnderPairWise(position, enableCompatible);
            }

            int storagePartNum = ExecUtils.calcStoragePartNum(hashCode, fullPartCount, isFullPartPowerOfTwo);
            if (prunePartitions.contains(storagePartNum)) {
                return -1;
            } else {
                return storagePartNum % partitionCount;
            }
        }
    }

    private static class SinglePartitionFunction implements RemotePartitionFunction {

        @Override
        public int getPartitionCount() {
            return 1;
        }

        @Override
        public int getPartition(Chunk page, int position) {
            return 0;
        }
    }

    private static class RandomPartitionFunction implements RemotePartitionFunction {
        private final int partitionCount;
        private final Random random;

        public RandomPartitionFunction(int partitionCount) {
            this.random = new Random();
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionCount() {
            return partitionCount;
        }

        @Override
        public int getPartition(Chunk page, int position) {
            return random.nextInt(this.partitionCount);
        }
    }
}
