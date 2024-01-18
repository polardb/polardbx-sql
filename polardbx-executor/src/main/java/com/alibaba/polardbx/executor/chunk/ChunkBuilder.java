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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class ChunkBuilder {

    private final BlockBuilder[] blockBuilders;
    private final List<DataType> types;
    private int declaredPositions;
    private final int chunkLimit;
    private ExecutionContext context;
    private final boolean enableDelay;
    private final boolean enableOssCompatible;

    public ChunkBuilder(List<DataType> types, int chunkLimit, ExecutionContext context) {
        this.types = types;
        this.context = context;

        this.enableDelay = context == null ? false : context.isEnableOssDelayMaterializationOnExchange();

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

    public Chunk fromPartition(List<Integer> assignedPositions, Chunk sourceChunk) {
        if (assignedPositions.isEmpty()) {
            return null;
        }

        final int sourceChunkLimit = assignedPositions.size();
        // pre-unbox
        int[] positions = assignedPositions.stream().mapToInt(i -> i).toArray();
        // for delay materialization
        int selSize = sourceChunkLimit;
        int[] newSelection;

        Block[] targetBlocks = new Block[sourceChunk.getBlockCount()];
        for (int channel = 0; channel < sourceChunk.getBlockCount(); channel++) {
            Block sourceBlock = sourceChunk.getBlock(channel);
            if (enableDelay
                && (sourceBlock instanceof SliceBlock)
                && selSize <= sourceBlock.getPositionCount()) {
                SliceBlock sliceBlock = (SliceBlock) sourceBlock;

                if (sliceBlock.getSelection() != null) {
                    int[] oldSelection = sliceBlock.getSelection();
                    newSelection = new int[selSize];
                    for (int position = 0; position < selSize; position++) {
                        newSelection[position] = oldSelection[positions[position]];
                    }
                } else {
                    newSelection = positions;
                }

                // delay for slice block
                targetBlocks[channel]
                    = new SliceBlock((SliceType) ((SliceBlock) sourceBlock).getType(), 0, selSize,
                    ((SliceBlock) sourceBlock).nulls(), ((SliceBlock) sourceBlock).offsets(),
                    ((SliceBlock) sourceBlock).data(), newSelection, enableOssCompatible);

            } else if (enableDelay
                && (sourceBlock instanceof DecimalBlock)
                && selSize <= sourceBlock.getPositionCount()) {
                DecimalBlock decimalBlock = (DecimalBlock) sourceBlock;

                if (decimalBlock.getSelection() != null) {
                    int[] oldSelection = decimalBlock.getSelection();
                    newSelection = new int[selSize];
                    for (int position = 0; position < selSize; position++) {
                        newSelection[position] = oldSelection[positions[position]];
                    }
                } else {
                    newSelection = positions;
                }

                // delay for decimal block
                targetBlocks[channel]
                    = new DecimalBlock(DataTypes.DecimalType, decimalBlock.getMemorySegments(),
                    decimalBlock.nulls(), decimalBlock.hasNull(), selSize,
                    newSelection, decimalBlock.getState());
            } else if (enableDelay
                && (sourceBlock instanceof DateBlock)
                && selSize <= sourceBlock.getPositionCount()) {
                DateBlock dateBlock = (DateBlock) sourceBlock;

                if (dateBlock.getSelection() != null) {
                    int[] oldSelection = dateBlock.getSelection();
                    newSelection = new int[selSize];
                    for (int position = 0; position < selSize; position++) {
                        newSelection[position] = oldSelection[positions[position]];
                    }
                } else {
                    newSelection = positions;
                }

                // delay for date block
                targetBlocks[channel]
                    = new DateBlock(0, selSize,
                    dateBlock.nulls(), dateBlock.getPacked(), dateBlock.getType(), dateBlock.getTimezone(),
                    newSelection);

            } else if (enableDelay
                && (sourceBlock instanceof IntegerBlock)
                && selSize <= sourceBlock.getPositionCount()) {
                IntegerBlock integerBlock = (IntegerBlock) sourceBlock;

                if (integerBlock.getSelection() != null) {
                    int[] oldSelection = integerBlock.getSelection();
                    newSelection = new int[selSize];
                    for (int position = 0; position < selSize; position++) {
                        newSelection[position] = oldSelection[positions[position]];
                    }
                } else {
                    newSelection = positions;
                }

                // delay for date block
                targetBlocks[channel]
                    = new IntegerBlock(integerBlock.getType(), integerBlock.intArray(), integerBlock.nulls(),
                    integerBlock.hasNull(), selSize, newSelection);
            } else {
                // normal
                for (int position : positions) {
                    sourceBlock.writePositionTo(position, blockBuilders[channel]);
                }
                targetBlocks[channel] = blockBuilders[channel].build();
            }
        }

        declarePosition(sourceChunkLimit);
        return new Chunk(sourceChunkLimit, targetBlocks);
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

    public void declarePosition(int positionCount) {
        declaredPositions = positionCount;
    }

    public BlockBuilder[] getBlockBuilders() {
        return blockBuilders;
    }

    public List<DataType> getTypes() {
        return types;
    }
}
