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

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.IOrderInvariantHash;
import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.executor.accumulator.state.NullableCheckSumGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.util.zip.CRC32;

/**
 * @author yaozhili
 */
public class CheckSumV2Accumulator implements Accumulator {
    private final DataType[] inputTypes;

    private final NullableCheckSumGroupState groupState;

    public CheckSumV2Accumulator(Aggregator aggregator, DataType[] rowInputType, int capacity) {
        int[] inputColumnIndexes = aggregator.getInputColumnIndexes();
        this.inputTypes = new DataType[inputColumnIndexes.length];
        for (int i = 0; i < inputTypes.length; i++) {
            inputTypes[i] = rowInputType[inputColumnIndexes[i]];
        }
        this.groupState = new NullableCheckSumGroupState(capacity, RevisableOrderInvariantHash.class);
    }

    @Override
    public DataType[] getInputTypes() {
        return inputTypes;
    }

    @Override
    public void appendInitValue() {
        this.groupState.appendNull();
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        // get crc result
        CRC32 crc = new CRC32();

        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            Block inputBlock = inputChunk.getBlock(i);
            if (inputBlock.isNull(position)) {
                crc.update(CrcAccumulator.NULL_TAG);
            } else {
                // Must keep compatible to columnar writers.
                int checksum = inputBlock.checksumV2(position);
                crc.update(new byte[] {
                    (byte) (checksum >>> 24), (byte) (checksum >>> 16), (byte) (checksum >>> 8), (byte) checksum});
            }
            crc.update(CrcAccumulator.SEPARATOR_TAG);
        }
        long crcResult = crc.getValue();

        // write to group state
        if (groupState.isNull(groupId)) {
            RevisableOrderInvariantHash revisableOrderInvariantHash = new RevisableOrderInvariantHash();
            revisableOrderInvariantHash.add(crcResult);
            groupState.set(groupId, revisableOrderInvariantHash);
        } else {
            IOrderInvariantHash revisableOrderInvariantHash = groupState.getHasher(groupId);
            revisableOrderInvariantHash.add(crcResult);
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
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
