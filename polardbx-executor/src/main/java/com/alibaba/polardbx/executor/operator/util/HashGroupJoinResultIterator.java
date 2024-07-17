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
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.base.Preconditions;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HashGroupJoinResultIterator implements AggResultIterator {

    private final List<Chunk> groupChunks;
    private final List<Chunk> valueChunks;

    private final AtomicInteger current = new AtomicInteger();
    private final int size;
    private final boolean filterNull;
    private final int chunkLimit;
    private final BitSet bitSet;
    private int currentRow = -1;

    public HashGroupJoinResultIterator(List<Chunk> groupChunks, List<Chunk> valueChunks, BitSet bitSet,
                                       boolean filterNull, int chunkLimit) {
        Preconditions.checkArgument(groupChunks.size() >= valueChunks.size());
        this.groupChunks = groupChunks;
        this.valueChunks = valueChunks;
        this.filterNull = filterNull;
        this.bitSet = bitSet;
        this.size = bitSet.size();
        if (size > 0) {
            currentRow = 0;
        }
        this.chunkLimit = chunkLimit;
    }

    @Override
    public Chunk nextChunk() {
        if (currentRow >= size) {
            return null;
        }
        int count = 0;
        for (int i = current.get(); i < groupChunks.size(); i++) {
            Chunk groupChunk = groupChunks.get(i);
            Chunk valueChunk = valueChunks.get(i);
            int valueBlockCount = valueChunk.getBlockCount();
            int groupBlockCount = groupChunk.getBlockCount();
            Block[] blocks = new Block[groupBlockCount + valueBlockCount];
            for (int j = 0; j < groupBlockCount; j++) {
                blocks[j] = groupChunk.getBlock(j);
                for (int k = 0; k < blocks[j].getPositionCount(); k++) {
                    if (bitSet.get(currentRow++)) {
                        final Object object = blocks[j].getObject(k);

                    }
                }
                current.incrementAndGet();
                count++;
            }
            for (int k = 0; i < valueBlockCount; i++) {
                blocks[k + groupBlockCount] = valueChunk.getBlock(k);
                count++;
            }
            if (count == chunkLimit) {
                break;
            }
            return new Chunk(blocks);
        }
        return null;
    }
}
