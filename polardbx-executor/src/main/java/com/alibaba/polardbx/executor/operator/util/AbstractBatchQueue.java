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
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

public abstract class AbstractBatchQueue {

    final List<DataType> columns;

    private final int batchSize;
    private BlockBuilder[] blockBuilders;
    private Chunk restChunk;
    private int restPos;
    private ExecutionContext context;

    public AbstractBatchQueue(int batchSize, List<DataType> columns, ExecutionContext context) {
        this.context = context;
        this.batchSize = batchSize;
        this.columns = columns;
        createBlockBuilders();
    }

    final void createBlockBuilders() {
        // Create all block builders by default
        blockBuilders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            blockBuilders[i] = BlockBuilders.create(columns.get(i), context);
        }
    }

    final Chunk buildChunkAndReset() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    protected abstract Chunk nextChunk();

    public Chunk pop() {
        if (restChunk == null) {
            restChunk = nextChunk();
            if (restChunk == null) {
                return null;
            }
            restPos = 0;
        }
        while (restChunk != null) {
            for (; restPos < restChunk.getPositionCount(); restPos++) {
                for (int i = 0; i < restChunk.getBlockCount(); i++) {
                    if (blockBuilders[i].getPositionCount() < batchSize) {
                        restChunk.getBlock(i).writePositionTo(restPos, blockBuilders[i]);
                    } else {
                        return buildChunkAndReset();
                    }
                }
            }
            restChunk = nextChunk();
            restPos = 0;
        }
        return buildChunkAndReset();
    }

    public int getBatchSize() {
        return batchSize;
    }
}
