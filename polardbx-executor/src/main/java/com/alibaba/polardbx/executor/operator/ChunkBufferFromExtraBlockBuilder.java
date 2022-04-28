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

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;

/**
 * IF NOT FOR COMPATIBILITY, PLEASE USE ChunkBufferFromRow INSTEAD.
 * This class is designed to be compatible with AbstractExecutor
 * blockBuilders just references in AbstractExecutor, this class not owner NOT take the ownership of blockBuilders
 * need to call flush in manual
 *
 */
public class ChunkBufferFromExtraBlockBuilder extends AbstractChunkBuffer {

    final BlockBuilder[] blockBuilders;
    final int chunkLimit;

    public ChunkBufferFromExtraBlockBuilder(BlockBuilder[] blockBuilders, int chunkLimit) {
        super();
        this.blockBuilders = blockBuilders;
        this.chunkLimit = chunkLimit;
    }

    @Override
    void flushToBuffer(boolean force) {
        if (!force && blockBuilders[0].getPositionCount() < chunkLimit) {
            return;
        }
        if (blockBuilders[0].getPositionCount() == 0) {
            return;
        }
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        bufferChunks.add(new Chunk(blocks));
    }

    @Override
    boolean allIsInBuffer() {
        return blockBuilders[0].getPositionCount() == 0;
    }
}
