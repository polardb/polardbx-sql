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

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HashAggResultIterator implements AggResultIterator {

    private final List<Chunk> groupChunks;
    private final List<Chunk> valueChunks;

    private final AtomicInteger current = new AtomicInteger();
    private final int size;

    public HashAggResultIterator(List<Chunk> groupChunks, List<Chunk> valueChunks) {
        Preconditions.checkArgument(groupChunks.size() == valueChunks.size());
        this.groupChunks = groupChunks;
        this.valueChunks = valueChunks;
        this.size = groupChunks.size();
    }

    @Override
    public Chunk nextChunk() {
        int index = current.getAndIncrement();
        if (index >= size) {
            return null;
        }

        Chunk groupChunk = groupChunks.get(index);
        Chunk valueChunk = valueChunks.get(index);

        int valueBlockCount = valueChunk.getBlockCount();
        int groupBlockCount = groupChunk.getBlockCount();

        Block[] blocks = new Block[groupBlockCount + valueBlockCount];
        for (int i = 0; i < groupBlockCount; i++) {
            blocks[i] = groupChunk.getBlock(i);
        }
        for (int i = 0; i < valueBlockCount; i++) {
            blocks[i + groupBlockCount] = valueChunk.getBlock(i);
        }
        return new Chunk(blocks);
    }
}
