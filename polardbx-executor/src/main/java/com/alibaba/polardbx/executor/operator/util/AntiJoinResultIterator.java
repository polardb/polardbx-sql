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

import java.util.List;

public class AntiJoinResultIterator {
    private List<Integer> matchedPosition;

    private ChunksIndex buildChunk;

    BlockBuilder[] blockBuilders;

    private final int chunkLimit;

    private volatile int buildPosition;

    /**
     * exclude
     */
    private final int endOffset;

    public AntiJoinResultIterator(List<Integer> matchedPosition, ChunksIndex buildChunk, BlockBuilder[] blockBuilders,
                                  int chunkLimit, int startOffset, int endOffset) {
        this.matchedPosition = matchedPosition;
        this.buildChunk = buildChunk;
        this.chunkLimit = chunkLimit;
        this.buildPosition = startOffset;
        this.blockBuilders = blockBuilders;
        this.endOffset = endOffset;
    }

    public Chunk nextChunk() {
        if (buildPosition >= endOffset) {
            return null;
        }

        while (buildPosition < endOffset) {
            buildAntiJoinRow(buildChunk, matchedPosition.get(buildPosition), blockBuilders);
            buildPosition++;
            // check buffered data is full
            if (currentPosition() >= chunkLimit) {
                return buildChunkAndReset(blockBuilders);
            }
        }
        return buildChunkAndReset(blockBuilders);
    }

    int currentPosition() {
        return blockBuilders[0].getPositionCount();
    }

    protected static void buildAntiJoinRow(ChunksIndex inputChunk, int position, BlockBuilder[] blockBuilders) {
        // inner side only
        long chunkIdAndPos = inputChunk.getAddress(position);
        for (int i = 0; i < blockBuilders.length; i++) {
            inputChunk.getChunk(SyntheticAddress.decodeIndex(chunkIdAndPos)).getBlock(i)
                .writePositionTo(SyntheticAddress.decodeOffset(chunkIdAndPos), blockBuilders[i]);
        }
    }

    protected static Chunk buildChunkAndReset(BlockBuilder[] blockBuilders) {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    public boolean finished() {
        return buildPosition >= endOffset;
    }
}

