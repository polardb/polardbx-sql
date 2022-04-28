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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBox;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.DecimalBoxGroupState;
import com.alibaba.polardbx.optimizer.state.GroupState;
import com.alibaba.polardbx.optimizer.state.NullableDecimalGroupState;

public class Decimal2DecimalSum extends AbstractAggregator {
    protected GroupState state;

    private Decimal cache;

    public Decimal2DecimalSum(int targetIndexes, boolean distinct, DataType inputType, DataType outputType,
                              int filterArg) {
        super(new int[] {targetIndexes}, distinct, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        state = new NullableDecimalGroupState(capacity);
        cache = new Decimal();
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Block block = inputChunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        DecimalBlock decimalBlock = (DecimalBlock) block;
        boolean isSimple = decimalBlock.isSimple();

        if (state instanceof DecimalBoxGroupState) {
            DecimalBoxGroupState boxGroupState = (DecimalBoxGroupState) state;

            if (isSimple) {
                // 1. best case: all decimal value in block is simple
                if (boxGroupState.isNull(groupId)) {
                    DecimalBox box = new DecimalBox();
                    int a1 = decimalBlock.fastInt1(position);
                    int a2 = decimalBlock.fastInt2(position);
                    int b = decimalBlock.fastFrac(position);
                    box.add(a1, a2, b);

                    boxGroupState.set(groupId, box);
                } else {
                    DecimalBox box = boxGroupState.getBox(groupId);

                    int a1 = decimalBlock.fastInt1(position);
                    int a2 = decimalBlock.fastInt2(position);
                    int b = decimalBlock.fastFrac(position);
                    box.add(a1, a2, b);
                }
            } else {
                // 2. bad case: a decimal value is not simple in the block

                // change state to
                this.state = boxGroupState.toDecimalGroupState();

                // do normal add
                normalAdd(groupId, position, decimalBlock);
            }
        } else if (state instanceof NullableDecimalGroupState) {
            // 3. normal case:

            normalAdd(groupId, position, decimalBlock);
        }
    }

    private void normalAdd(int groupId, int position, DecimalBlock decimalBlock) {
        NullableDecimalGroupState decimalGroupState = (NullableDecimalGroupState) state;
        Decimal value = decimalBlock.getDecimal(position);
        if (decimalGroupState.isNull(groupId)) {
            // initialize the operand (not null)
            decimalGroupState.set(groupId, value.copy());
        } else {
            Decimal beforeValue = decimalGroupState.get(groupId);

            // avoid reset memory to 0
            FastDecimalUtils.add(
                beforeValue.getDecimalStructure(),
                value.getDecimalStructure(),
                cache.getDecimalStructure(),
                false);

            // swap variants to avoid allocating memory
            Decimal afterValue = cache;
            cache = beforeValue;

            decimalGroupState.set(groupId, afterValue);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (state instanceof DecimalBoxGroupState) {
            if (((DecimalBoxGroupState)state).isNull(groupId)) {
                bb.appendNull();
            } else {
                bb.writeDecimal(((DecimalBoxGroupState)state).get(groupId));
            }
        } else if (state instanceof NullableDecimalGroupState) {
            if (((NullableDecimalGroupState)state).isNull(groupId)) {
                bb.appendNull();
            } else {
                bb.writeDecimal(((NullableDecimalGroupState)state).get(groupId));
            }
        }
    }

    @Override
    public void appendInitValue() {
        if (state instanceof DecimalBoxGroupState) {
            ((DecimalBoxGroupState) state).appendNull();
        } else if (state instanceof NullableDecimalGroupState) {
            ((NullableDecimalGroupState) state).appendNull();
        }
    }

    @Override
    public void resetToInitValue(int groupId) {
        if (state instanceof DecimalBoxGroupState) {
            ((DecimalBoxGroupState) state).setNull(groupId);
        } else if (state instanceof NullableDecimalGroupState) {
            ((NullableDecimalGroupState) state).setNull(groupId);
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}