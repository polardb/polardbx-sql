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
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableLongGroupState;

import java.util.HashMap;

public abstract class LittleNum2DecimalSum extends AbstractAggregator {
    private NullableLongGroupState partialGroupState;
    private HashMap<Integer, Decimal> overflowToDecimal;

    public LittleNum2DecimalSum(int targetIndexes, boolean distinct, DataType inputType, DataType outputType,
                                int filterArg) {
        super(new int[] {targetIndexes}, distinct, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        partialGroupState = new NullableLongGroupState(capacity);
        overflowToDecimal = null;
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Block block = inputChunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        final long value = getLong(block, position);
        if (partialGroupState.isNull(groupId)) {
            partialGroupState.set(groupId, value);
        } else {
            long oldValue = partialGroupState.get(groupId);
            long sumValue = oldValue + value;
            // HD 2-12 Overflow iff both arguments have the opposite sign of the result
            if (((oldValue ^ sumValue) & (value ^ sumValue)) < 0) {
                if (overflowToDecimal == null) {
                    overflowToDecimal = new HashMap<>();
                }
                Decimal previousSum = overflowToDecimal.getOrDefault(groupId, Decimal.ZERO);
                overflowToDecimal.put(groupId,
                        previousSum.add(Decimal.fromLong(oldValue)).add(Decimal.fromLong(value)));
                partialGroupState.set(groupId, 0L);
            } else {
                partialGroupState.set(groupId, sumValue);
            }
        }
    }

    abstract long getLong(Block block, int position);

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (partialGroupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            if (overflowToDecimal == null || !overflowToDecimal.containsKey(groupId)) {
                bb.writeDecimal(Decimal.fromLong(partialGroupState.get(groupId)));
            } else {
                bb.writeDecimal(overflowToDecimal.get(groupId).add(Decimal.fromLong(partialGroupState.get(groupId))));
            }
        }
    }

    @Override
    public void appendInitValue() {
        partialGroupState.appendNull();
    }

    @Override
    public void resetToInitValue(int groupId) {
        partialGroupState.setNull(groupId);
        if (overflowToDecimal != null) {
            overflowToDecimal.remove(groupId);
        }
    }

    @Override
    public long estimateSize() {
        return partialGroupState.estimateSize() + (overflowToDecimal == null ? 0 :
                overflowToDecimal.size() * (Integer.BYTES + 29));
    }
}
