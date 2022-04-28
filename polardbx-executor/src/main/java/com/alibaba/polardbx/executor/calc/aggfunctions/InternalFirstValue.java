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

package com.alibaba.polardbx.executor.calc.aggfunctions;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.NullBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuanqin on 18/1/22.
 */
public class InternalFirstValue extends AbstractAggregator {

    private TypedBlockBuffer typedBlockBuffer;

    private ExecutionContext context;

    private static final int SEGMENT_SIZE = 1024;

    /**
     * use fixScalarAggValue with not null first value to fix append only typedBlockBuffer
     *
     * <p>example:
     * <p>
     * [table t data]:
     * ------------
     * | id | age |
     * -----+------
     * | 1  | 9   |
     * ------------
     * <p>
     * [sql]:
     * select max(id), age from t;
     * <p>
     * handle the special case:
     * 1. partition table with only one row data
     * 2. scalar agg with first value
     * 3. two phase agg were generated
     * <p>
     * There will be possible to produce follow unexpected result for two phase scalar first value agg :
     * ------------
     * | id | age |
     * -----+------
     * | 1  | NULL|
     * ------------
     * <p>
     * so we need non-null first value to fix scalar agg
     */
    private Object fixScalarAggValue;

    public InternalFirstValue(int index, DataType outType, int filterArg, ExecutionContext context) {
        super(new int[] {index}, false, new DataType[] {outType}, outType, filterArg);
        this.context = context;
        this.typedBlockBuffer = new TypedBlockBuffer(outType, SEGMENT_SIZE);
    }

    @Override
    public void open(int capacity) {
        this.typedBlockBuffer = new TypedBlockBuffer(returnType, SEGMENT_SIZE);
    }

    @Override
    public void appendInitValue() {
        // delay append value to accumulate, because first_value can only append once
    }

    @Override
    public void resetToInitValue(int groupId) {
        this.typedBlockBuffer = new TypedBlockBuffer(returnType, SEGMENT_SIZE);
        fixScalarAggValue = null;
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        Block block = chunk.getBlock(aggIndexInChunk[0]);
        if (groupId == 0 && fixScalarAggValue == null && !block.isNull(position)) {
            fixScalarAggValue = block.getObject(position);
        }
        if (groupId < typedBlockBuffer.size()) {
            // pass
        } else if (groupId == typedBlockBuffer.size()) {
            // do append value here
            typedBlockBuffer.appendValue(block, position);
        } else {
            throw new AssertionError("impossible case");
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        Object value;
        if (typedBlockBuffer.size() == 0 && groupId == 0) {
            /*
             * This line handles a very special case: this IS a scalar agg and there IS NO input rows.
             * In this case `appendInitValue()` was called but `accumulate()` was not, which leads to
             * an empty buffer. We put a NULL here to make it behave correctly.
             */
            typedBlockBuffer.appendValue(new NullBlock(1), 0);
            value = typedBlockBuffer.get(groupId);
        } else if (typedBlockBuffer.size() == 1) {
            value = fixScalarAggValue;
        } else {
            value = typedBlockBuffer.get(groupId);
        }
        bb.writeObject(value);
    }

    @Override
    public long estimateSize() {
        return typedBlockBuffer.estimateSize();
    }

    public class TypedBlockBuffer {

        private BlockBuilder blockBuilder;
        private final int blockSize;

        private int currentSize;
        private final List<Block> blocks = new ArrayList<>();
        private long estimateSize = 0;

        private TypedBlockBuffer(DataType dataType, int blockSize) {
            this.blockBuilder = BlockBuilders.create(dataType, context);
            this.blockSize = blockSize;
        }

        public Object get(int position) {
            return blockOf(position).getObject(offsetOf(position));
        }

        public void appendValue(Block block, int position) {
            // Block fulfilled before appending
            if (currentSize == blockSize) {
                Block buildingBlock = getBuildingBlock();
                blocks.add(buildingBlock);
                estimateSize += buildingBlock.estimateSize();
                blockBuilder = blockBuilder.newBlockBuilder();
                currentSize = 0;
            }

            block.writePositionTo(position, blockBuilder);
            currentSize++;
        }

        private Block blockOf(int position) {
            int chunkId = position / blockSize;
            if (chunkId < blocks.size()) {
                return blocks.get(chunkId);
            } else {
                return getBuildingBlock();
            }
        }

        public int size() {
            return currentSize + blocks.size() * blockSize;
        }

        private int offsetOf(int position) {
            return position % blockSize;
        }

        private Block getBuildingBlock() {
            return blockBuilder.build();
        }

        public long estimateSize() {
            return estimateSize;
        }
    }
}
