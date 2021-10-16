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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

public class ChunkBuilder {

    private final BlockBuilder[] blockBuilders;
    private final List<DataType> types;
    private int declaredPositions;
    private final int chunkLimit;
    private ExecutionContext context;

    public ChunkBuilder(List<DataType> types, int chunkLimit, ExecutionContext context) {
        this.types = types;
        this.context = context;
        blockBuilders = new BlockBuilder[types.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = BlockBuilders.create(types.get(i), context);
        }
        this.chunkLimit = chunkLimit;
    }

    public boolean isEmpty() {
        return declaredPositions == 0;
    }

    public boolean isFull() {
        return declaredPositions == Integer.MAX_VALUE || declaredPositions >= chunkLimit;
    }

    public Chunk build() {
        if (blockBuilders.length == 0) {
            return new Chunk(declaredPositions);
        }

        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
            if (blocks[i].getPositionCount() != declaredPositions) {
                throw new IllegalStateException(String
                    .format("Declared positions (%s) does not match block %s's number of entries (%s)",
                        declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        return new Chunk(declaredPositions, blocks);
    }

    public void reset() {
        if (isEmpty()) {
            return;
        }

        declaredPositions = 0;

        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
    }

    public void appendTo(Block block, int channel, int position) {
        BlockBuilder blockBuilder = blockBuilders[channel];
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            block.writePositionTo(position, blockBuilder);
        }
    }

    public DataType getType(int channel) {
        return types.get(channel);
    }

    public BlockBuilder getBlockBuilder(int channel) {
        return blockBuilders[channel];
    }

    public void declarePosition() {
        declaredPositions++;
    }

    public BlockBuilder[] getBlockBuilders() {
        return blockBuilders;
    }

    public List<DataType> getTypes() {
        return types;
    }
}
