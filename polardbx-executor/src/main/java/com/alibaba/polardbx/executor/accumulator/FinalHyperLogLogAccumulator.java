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

package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.NullableHyperLogLogGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

public class FinalHyperLogLogAccumulator implements Accumulator {
    private final DataType[] inputTypes;

    private final NullableHyperLogLogGroupState groupState;

    public FinalHyperLogLogAccumulator(Aggregator aggregator, DataType[] rowInputType, int capacity) {
        int[] inputColumnIndexes = aggregator.getInputColumnIndexes();
        this.inputTypes = new DataType[inputColumnIndexes.length];
        for (int i = 0; i < inputTypes.length; i++) {
            inputTypes[i] = rowInputType[inputColumnIndexes[i]];
        }
        this.groupState = new NullableHyperLogLogGroupState(capacity);
    }

    @Override
    public DataType[] getInputTypes() {
        return inputTypes;
    }

    @Override
    public void appendInitValue() {
        this.groupState.append();
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int startIndexIncluded, int endIndexExcluded) {
        byte[] hll;
        if (groupState.isNull(groupId)) {
            hll = new byte[HyperLogLogUtil.HLL_REGBYTES_DE];
            groupState.set(groupId, hll);
        } else {
            hll = groupState.getHll(groupId);
        }

        Block inputBlock = inputChunk.getBlock(0);

        for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
            if (inputBlock.isNull(i)) {
                return;
            }
            HyperLogLogUtil.merge(hll, inputBlock.getByteArray(i));
        }
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Block inputBlock = inputChunk.getBlock(0);
        if (inputBlock.isNull(position)) {
            return;
        }

        byte[] hll;
        if (groupState.isNull(groupId)) {
            hll = new byte[HyperLogLogUtil.HLL_REGBYTES_DE];
            groupState.set(groupId, hll);
        } else {
            hll = groupState.getHll(groupId);
        }
        HyperLogLogUtil.merge(hll, inputBlock.getByteArray(position));
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.writeLong(0);
        } else {
            bb.writeLong(groupState.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
