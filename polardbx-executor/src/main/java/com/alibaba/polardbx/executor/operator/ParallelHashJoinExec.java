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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.util.IntBloomFilter;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

/**
 * Parallel Hash-Join Executor
 *
 */
public class ParallelHashJoinExec extends AbstractHashJoinExec {

    private Synchronizer shared;

    private boolean finished;
    private ListenableFuture<?> blocked;

    protected boolean buildOuterInput;

    private int buildChunkSize = 0;

    private int operatorIndex = -1;

    private boolean probeInputIsFinish = false;

    public ParallelHashJoinExec(Synchronizer synchronizer,
                                Executor outerInput,
                                Executor innerInput,
                                JoinRelType joinType,
                                boolean maxOneRow,
                                List<EquiJoinKey> joinKeyTuples,
                                IExpression otherCondition,
                                List<IExpression> antiJoinOperands,
                                boolean buildOuterInput,
                                ExecutionContext context,
                                int operatorIndex) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeyTuples, otherCondition, antiJoinOperands, context);
        this.shared = Preconditions.checkNotNull(synchronizer);
        this.finished = false;
        this.blocked = ProducerExecutor.NOT_BLOCKED;

        this.buildOuterInput = buildOuterInput;
        this.operatorIndex = operatorIndex;
        if (buildOuterInput) {
            this.shared.recordOperatorIds(operatorIndex);
        }
    }

    @Override
    public void openConsume() {
        Preconditions.checkArgument(shared != null, "reopen not supported yet");
        // Use one shared memory pool but a local memory allocator
        this.memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ParallelHashJoinExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocator = memoryPool.getMemoryAllocatorCtx();
    }

    @Override
    public void doOpen() {
        if (!passThrough && passNothing) {
            //避免初始化probe side, minor optimizer
            return;
        }
        super.doOpen();
    }

    @Override
    public void buildConsume() {
        if (memoryPool != null) {
            int partition = shared.buildCount.getAndIncrement();
            if (partition < shared.numPartitions) {
                shared.buildHashTable(partition, memoryAllocator);
            }
            // Copy the built hash-table from shared states into this executor
            this.buildChunks = shared.builderChunks;
            this.buildKeyChunks = shared.builderKeyChunks;
            this.hashTable = shared.hashTable;
            this.positionLinks = shared.positionLinks;
            this.bloomFilter = shared.bloomFilter;
            if (buildChunks.isEmpty() && joinType == JoinRelType.INNER) {
                passNothing = true;
            }
            if (semiJoin) {
                doSpecialCheckForSemiJoin();
            }

            this.buildChunkSize = this.buildChunks.getPositionCount();
        }
    }

    @Override
    Chunk doNextChunk() {
        // Special path for pass-through or pass-nothing mode
        if (passThrough) {
            return nextProbeChunk();
        } else if (passNothing) {
            return null;
        }

        if (buildOuterInput && shared.joinNullRowBitSet == null) {
            shared.buildNullBitSets(buildChunkSize);
        }

        return super.doNextChunk();
    }

    @Override
    Chunk nextProbeChunk() {
        Chunk ret = getProbeInput().nextChunk();
        if (ret == null) {
            probeInputIsFinish = getProbeInput().produceIsFinished();
            blocked = getProbeInput().produceIsBlocked();
        }
        return ret;
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        synchronized (shared) {
            shared.builderChunks.addChunk(inputChunk);

            Chunk keyChunk = getBuildKeyChunkGetter().apply(inputChunk);
            shared.builderKeyChunks.addChunk(keyChunk);

            memoryAllocator.allocateReservedMemory(inputChunk.estimateSize() + keyChunk.estimateSize());
        }
    }

    @Override
    public boolean nextJoinNullRows() {
        synchronized (shared) {
            if (!shared.operatorIds.isEmpty()) {
                return false;
            }
        }
        int matchedPosition = shared.nextUnmatchedPosition();
        if (matchedPosition == -1) {
            return false;
        }
        // first outer side, then inner side
        int col = 0;

        if (joinType != JoinRelType.RIGHT) {
            for (int j = 0; j < outerInput.getDataTypes().size(); j++) {
                buildChunks.writePositionTo(j, matchedPosition, blockBuilders[col++]);
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            for (int j = 0; j < rightColumns; j++) {
                blockBuilders[col++].appendNull();
            }
        } else {
            for (int j = 0; j < innerInput.getDataTypes().size(); j++) {
                blockBuilders[col++].appendNull();
            }
            for (int j = 0; j < outerInput.getDataTypes().size(); j++) {
                buildChunks.writePositionTo(j, matchedPosition, blockBuilders[col++]);
            }
        }
        assert col == blockBuilders.length;
        return true;
    }

    @Override
    protected void afterProcess(Chunk outputChunk) {
        super.afterProcess(outputChunk);
        if (buildOuterInput) {
            if (probeChunk == null) {
                if (probeInputIsFinish) {
                    if (shared.consumeInputIsFinish(operatorIndex) && shared.finishIterator()) {
                        finished = true;
                        blocked = ProducerExecutor.NOT_BLOCKED;
                    } else {
                        finished = false;
                        blocked = ProducerExecutor.NOT_BLOCKED;
                    }
                } else {
                    finished = false;
                }
            } else {
                finished = false;
                blocked = ProducerExecutor.NOT_BLOCKED;
            }
        } else {
            if (outputChunk == null) {
                finished = probeInputIsFinish;
            } else {
                finished = false;
                blocked = ProducerExecutor.NOT_BLOCKED;
            }
        }
    }

    @Override
    protected void buildJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first outer side, then inner side
        if (buildOuterInput) {
            shared.markUsedKeys(matchedPosition);
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            for (int i = 0; i < rightColumns; i++) {
                probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
            }
            assert col == blockBuilders.length;
        } else {
            super.buildJoinRow(chunksIndex, probeInputChunk, position, matchedPosition);
        }
    }

    @Override
    protected void buildRightJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first inner side, then outer side
        if (buildOuterInput) {
            shared.markUsedKeys(matchedPosition);
            int col = 0;
            for (int i = 0; i < innerInput.getDataTypes().size(); i++) {
                probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
            }
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
            }
            assert col == blockBuilders.length;
        } else {
            super.buildRightJoinRow(chunksIndex, probeInputChunk, position, matchedPosition);
        }
    }

    @Override
    void doClose() {
        // Release the reference to shared hash table etc.
        if (shared != null) {
            this.shared.consumeInputIsFinish(operatorIndex);
            this.shared = null;
        }
        super.doClose();
    }

    @Override
    public boolean produceIsFinished() {
        return finished || passNothing;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    @Override
    public Executor getBuildInput() {
        if (buildOuterInput) {
            return outerInput;
        } else {
            return innerInput;
        }
    }

    @Override
    public Executor getProbeInput() {
        if (buildOuterInput) {
            return innerInput;
        } else {
            return outerInput;
        }
    }

    @Override
    ChunkConverter getBuildKeyChunkGetter() {
        if (buildOuterInput) {
            return outerKeyChunkGetter;
        } else {
            return innerKeyChunkGetter;
        }
    }

    @Override
    ChunkConverter getProbeKeyChunkGetter() {
        if (buildOuterInput) {
            return innerKeyChunkGetter;
        } else {
            return outerKeyChunkGetter;
        }
    }

    @Override
    protected boolean checkJoinCondition(
        ChunksIndex chunksIndex, Chunk outerChunk, int outerPosition, int innerPosition) {
        if (condition == null) {
            return true;
        }

        final Row outerRow = outerChunk.rowAt(outerPosition);
        final Row innerRow = buildChunks.rowAt(innerPosition);

        Row leftRow = joinType.leftSide(outerRow, innerRow);
        Row rightRow = joinType.rightSide(outerRow, innerRow);
        if (buildOuterInput) {
            Row leftRowTemp = leftRow;
            leftRow = rightRow;
            rightRow = leftRowTemp;
        }
        JoinRow joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);

        return checkJoinCondition(joinRow);
    }

    @Override
    protected boolean outputNullRowInTime() {
        return !buildOuterInput;
    }

    public static class Synchronizer {

        private final int numPartitions;

        private final AtomicInteger buildCount = new AtomicInteger();

        // Shared States
        private final ChunksIndex builderChunks = new ChunksIndex();
        private final ChunksIndex builderKeyChunks = new ChunksIndex();

        private ConcurrentRawHashTable hashTable;
        private int[] positionLinks;
        private IntBloomFilter bloomFilter;
        private boolean alreadyUseRuntimeFilter;

        private BitSet joinNullRowBitSet;
        private int maxIndex = -1;
        private int nextIndexId = 0;

        private final Set<Integer> operatorIds = new HashSet<>();

        public Synchronizer(int numPartitions, boolean alreadyUseRuntimeFilter) {
            this.numPartitions = numPartitions;
            this.alreadyUseRuntimeFilter = alreadyUseRuntimeFilter;
        }

        private synchronized void initHashTable(MemoryAllocatorCtx ctx) {
            if (hashTable == null) {
                final int size = builderKeyChunks.getPositionCount();
                hashTable = new ConcurrentRawHashTable(size);

                positionLinks = new int[size];
                Arrays.fill(positionLinks, LIST_END);

                ctx.allocateReservedMemory(hashTable.estimateSize());
                ctx.allocateReservedMemory(SizeOf.sizeOf(positionLinks));

                if (!alreadyUseRuntimeFilter && size <= BLOOM_FILTER_ROWS_LIMIT_FOR_PARALLEL && size > 0) {
                    bloomFilter = IntBloomFilter.create(size);
                    ctx.allocateReservedMemory(bloomFilter.sizeInBytes());
                }
            }
        }

        private void buildHashTable(int partition, MemoryAllocatorCtx ctx) {
            initHashTable(ctx);
            final int partitionSize = -Math.floorDiv(-builderKeyChunks.getChunkCount(), numPartitions);
            final int startChunkId = partitionSize * partition;

            if (startChunkId >= builderKeyChunks.getChunkCount()) {
                return; // skip invalid chunk ranges
            }

            final int endChunkId = Math.min(startChunkId + partitionSize, builderKeyChunks.getChunkCount());
            final int startPosition = builderKeyChunks.getChunkOffset(startChunkId);
            final int endPosition = builderKeyChunks.getChunkOffset(endChunkId);

            int position = startPosition;
            for (int chunkId = startChunkId; chunkId < endChunkId; ++chunkId) {
                final Chunk keyChunk = builderKeyChunks.getChunk(chunkId);
                buildOneChunk(keyChunk, position, hashTable, positionLinks, bloomFilter);
                position += keyChunk.getPositionCount();
            }
            assert position == endPosition;
        }

        private synchronized void recordOperatorIds(int operatorId) {
            this.operatorIds.add(operatorId);
        }

        private synchronized boolean consumeInputIsFinish(int operatorId) {
            this.operatorIds.remove(operatorId);
            return this.operatorIds.isEmpty();
        }

        private synchronized void buildNullBitSets(int buildSize) {
            if (joinNullRowBitSet == null) {
                joinNullRowBitSet = new BitSet(buildSize);
                maxIndex = buildSize;
            }
        }

        private synchronized void markUsedKeys(int matchedPosition) {
            joinNullRowBitSet.set(matchedPosition);
        }

        private synchronized int nextUnmatchedPosition() {
            if (maxIndex > nextIndexId) {
                int unmatchedPosition = joinNullRowBitSet.nextClearBit(nextIndexId);
                nextIndexId = unmatchedPosition + 1;
                if (maxIndex > unmatchedPosition) {
                    return unmatchedPosition;
                } else {
                    return -1;
                }

            } else {
                return -1;
            }
        }

        private synchronized boolean finishIterator() {
            return nextIndexId >= maxIndex;
        }
    }
}
