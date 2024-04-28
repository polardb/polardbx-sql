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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

public class ChunkBuilder {

    private final BlockBuilder[] blockBuilders;
    private final List<DataType> types;
    private int declaredPositions;
    private final int chunkLimit;
    private ExecutionContext context;
    private final boolean enableBlockBuilderBatchWriting;
    private final boolean enableOssCompatible;
    private final boolean useBlockWriter;
    private ObjectPools objectPools;

    public ChunkBuilder(List<DataType> types, int chunkLimit, ExecutionContext context, ObjectPools objectPools) {
        this.types = types;
        this.context = context;
        this.objectPools = objectPools;

        this.enableBlockBuilderBatchWriting =
            context == null ? false :
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_BLOCK_BUILDER_BATCH_WRITING);
        this.useBlockWriter =
            context == null ? false : context.getParamManager().getBoolean(ConnectionParams.ENABLE_DRIVER_OBJECT_POOL);

        this.enableOssCompatible = context == null ? true : context.isEnableOssCompatible();
        blockBuilders = new BlockBuilder[types.size()];
        if (useBlockWriter && objectPools != null) {
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = BatchBlockWriter.create(types.get(i), context, chunkLimit, objectPools);
            }
        } else {
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = BlockBuilders.create(types.get(i), context);
            }
        }

        this.chunkLimit = chunkLimit;
    }

    public ChunkBuilder(List<DataType> types, int chunkLimit, ExecutionContext context) {
        this.types = types;
        this.context = context;

        this.useBlockWriter = false;
        this.enableBlockBuilderBatchWriting =
            context == null ? false :
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_BLOCK_BUILDER_BATCH_WRITING);

        this.enableOssCompatible = context == null ? true : context.isEnableOssCompatible();
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

        if (useBlockWriter && objectPools != null) {
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = blockBuilders[i].newBlockBuilder(objectPools, chunkLimit);
            }
        } else {
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = blockBuilders[i].newBlockBuilder();
            }
        }

    }

    public void appendTo(Block block, int channel, int[] selection, final int offsetInSelection,
                         final int positionCount) {
        BlockBuilder blockBuilder = blockBuilders[channel];
        if (blockBuilder instanceof BatchBlockWriter) {
            ((BatchBlockWriter) blockBuilder).copyBlock(block, selection, offsetInSelection, 0, positionCount);
        } else {
            if (enableBlockBuilderBatchWriting) {
                block.writePositionTo(selection, offsetInSelection, positionCount, blockBuilder);
            } else {
                for (int i = 0; i < positionCount; i++) {
                    block.writePositionTo(selection[i + offsetInSelection], blockBuilder);
                }
            }
        }
    }

    public void appendTo(Block block, int channel, int position) {
        BlockBuilder blockBuilder = blockBuilders[channel];
        // appendNull may have additional operations in certain block types
        block.writePositionTo(position, blockBuilder);
    }

    public DataType getType(int channel) {
        return types.get(channel);
    }

    public BlockBuilder getBlockBuilder(int channel) {
        return blockBuilders[channel];
    }

    public void updateDeclarePosition(int update) {
        declaredPositions += update;
    }

    public void declarePosition() {
        declaredPositions++;
    }

    public void declarePosition(int positionCount) {
        declaredPositions = positionCount;
    }

    public int getDeclarePosition() {
        return declaredPositions;
    }

    public BlockBuilder[] getBlockBuilders() {
        return blockBuilders;
    }

    public List<DataType> getTypes() {
        return types;
    }
}
