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
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkBuilder;
import com.alibaba.polardbx.optimizer.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.mpp.operator.LocalBucketPartitionFunction;
import com.alibaba.polardbx.executor.mpp.operator.PartitionFunction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;

public class BucketDivideChunkBuffer extends AbstractChunkBuffer implements Executor {

    private final int bucketNum;
    private final ChunkBufferFromRow[] bucketBuffers;
    private final PartitionFunction bucketPartitionFunction;
    private final ChunkConverter keyConverter;
    private final List<DataType> types;

    public BucketDivideChunkBuffer(int bucketNum, int partitionCount, int partitionIndex,
                                   ChunkConverter keyConverter, List<DataType> types, int chunkLimit,
                                   ExecutionContext context) {
        this.bucketNum = bucketNum;
        this.types = types;
        this.bucketBuffers = new ChunkBufferFromRow[bucketNum];
        int bucketChunkLimit = Math.max(16, chunkLimit / bucketNum);
        for (int i = 0; i < bucketNum; i++) {
            this.bucketBuffers[i] = new ChunkBufferFromRow(new ChunkBuilder(types, bucketChunkLimit, context));
        }
        this.bucketPartitionFunction =
            new LocalBucketPartitionFunction(bucketNum, partitionCount, partitionIndex);
        this.keyConverter = keyConverter;
    }

    @Override
    void flushToBuffer(boolean force) {
        if (force) {
            for (ChunkBufferFromRow buffer : bucketBuffers) {
                buffer.flushToBuffer(true);
            }
        }
        for (ChunkBufferFromRow buffer : bucketBuffers) {
            while (buffer.hasNextChunk()) {
                bufferChunks.add(buffer.nextChunk());
            }
        }
    }

    @Override
    boolean allIsInBuffer() {
        for (ChunkBufferFromRow buffer : bucketBuffers) {
            if (!buffer.produceIsFinished()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addChunk(Chunk chunk) {

        IntList[] partitionAssignments = new IntList[bucketNum];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }

        Chunk keyChunk = keyConverter.apply(chunk);

        for (int position = 0; position < keyChunk.getPositionCount(); position++) {
            int bucket = bucketPartitionFunction.getPartition(keyChunk, position);
            partitionAssignments[bucket].add(position);
        }

        for (int bucket = 0; bucket < bucketNum; bucket++) {
            List<Integer> positions = partitionAssignments[bucket];
            if (positions.isEmpty()) {
                continue;
            }
            for (Integer pos : positions) {
                bucketBuffers[bucket].addRow(chunk, pos);
            }
            while (bucketBuffers[bucket].hasNextChunk()) {
                bufferChunks.add(bucketBuffers[bucket].nextChunk());
            }
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return types;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public List<Executor> getInputs() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return Executor.NOT_BLOCKED;
    }
}
