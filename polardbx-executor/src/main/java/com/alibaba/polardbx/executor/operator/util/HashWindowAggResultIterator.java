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

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HashWindowAggResultIterator implements AggResultIterator {
    private final List<Chunk> valueChunks;

    private final AtomicInteger current = new AtomicInteger();
    private final int inputChunkSize;

    private final List<Chunk> inputChunks;
    private final List<int[]> groupIds;

    private BlockBuilder[] valueBlockBuilders;

    private final int chunkSize;

    public HashWindowAggResultIterator(List<Chunk> valueChunks,
                                       List<Chunk> inputChunks, List<int[]> groupIds,
                                       BlockBuilder[] blockBuilders, int chunkSize) {
        Preconditions.checkArgument(groupIds.size() == inputChunks.size(),
            "size of input chunk should be same with group id list");
        this.valueChunks = valueChunks;
        this.inputChunks = inputChunks;
        this.groupIds = groupIds;
        this.inputChunkSize = inputChunks.size();
        this.valueBlockBuilders = blockBuilders;
        this.chunkSize = chunkSize;
    }

    @Override
    public Chunk nextChunk() {
        int index = current.getAndIncrement();
        if (index >= inputChunkSize) {
            return null;
        }

        Chunk inputChunk = inputChunks.get(index);

        int inputBlockCount = inputChunk.getBlockCount();
        int valueBlockCount = valueBlockBuilders.length;

        Block[] results = new Block[inputBlockCount + valueBlockCount];
        for (int i = 0; i < inputBlockCount; i++) {
            results[i] = inputChunk.getBlock(i);
        }

        int[] groupId = groupIds.get(index);

        for (int pos = 0; pos < inputChunk.getPositionCount(); ++pos) {
            for (int i = 0; i < valueBlockCount; ++i) {
                valueChunks.get(groupId[pos] / chunkSize).getBlock(i)
                    .writePositionTo(groupId[pos] % chunkSize, valueBlockBuilders[i]);
            }
        }

        Block[] valueBlocks = new Block[valueBlockBuilders.length];
        for (int i = 0; i < valueBlocks.length; i++) {
            valueBlocks[i] = valueBlockBuilders[i].build();
        }

        for (int i = 0; i < valueBlockCount; i++) {
            results[i + inputBlockCount] = valueBlocks[i];
        }

        for (int i = 0; i < valueBlockBuilders.length; i++) {
            valueBlockBuilders[i] = valueBlockBuilders[i].newBlockBuilder();
        }
        return new Chunk(results);
    }
}
