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

import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.executor.accumulator.state.NullableCheckSumGroupState;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.aliyun.oss.common.utils.CRC64;

import java.util.List;

public class CheckSumAccumulator extends AbstractAggregator {
    private final List<DataType> inputTypes;

    private NullableCheckSumGroupState groupState;

    private final static byte SEPARATOR_TAG = (byte) 255;
    private final static byte NULL_TAG = (byte) 254;

    public CheckSumAccumulator(int[] aggTargetIndexes, DataType outType, int filterArg, List<DataType> inputTypes) {
        super(aggTargetIndexes, false, new DataType[] {outType}, outType, filterArg);
        this.inputTypes = inputTypes;
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableCheckSumGroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        this.groupState.appendNull();
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        // get crc result
        CRC64 crc = new CRC64();

        for (int i = 0; i < aggIndexInChunk.length; i++) {
            Block inputBlock = inputChunk.getBlock(aggIndexInChunk[i]);
            if (inputBlock.isNull(position)) {
                crc.update(NULL_TAG);
            } else {
                int checksum = inputBlock.checksum(position);
                crc.update(checksum);
            }
            crc.update(SEPARATOR_TAG);
        }
        long crcResult = crc.getValue();

        // write to group state
        if (groupState.isNull(groupId)) {
            OrderInvariantHasher orderInvariantHasher = new OrderInvariantHasher();
            orderInvariantHasher.add(crcResult);
            groupState.set(groupId, orderInvariantHasher);
        } else {
            OrderInvariantHasher orderInvariantHasher = groupState.getHasher(groupId);
            orderInvariantHasher.add(crcResult);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            bb.writeLong(groupState.get(groupId));
        }
    }

    @Override
    public void resetToInitValue(int groupId) {
        this.groupState.set(groupId, null);
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
