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
import com.aliyun.oss.common.utils.CRC64;

public class HyperLogLogAccumulator implements Accumulator {
    protected final DataType[] inputTypes;

    protected final NullableHyperLogLogGroupState groupState;

    protected final static byte SEPARATOR_TAG = (byte) 255;
    protected final static byte NULL_TAG = (byte) 254;

    public HyperLogLogAccumulator(Aggregator aggregator, DataType[] rowInputType, int capacity) {
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
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        byte[] hll;
        if (groupState.isNull(groupId)) {
            hll = new byte[HyperLogLogUtil.HLL_REGBYTES_DE];
            groupState.set(groupId, hll);
        } else {
            hll = groupState.getHll(groupId);
        }

        // only one column, no need to use crc
        if (inputChunk.getBlockCount() == 1) {
            HyperLogLogUtil.hllSet(hll, inputChunk.getBlock(0).hashCodeUseXxhash(position));
            return;
        }

        // get crc result
        //CRC32 crc = new CRC32();
        CRC64 crc = new CRC64();
        byte[] bytes = new byte[8];
        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            Block inputBlock = inputChunk.getBlock(i);
            if (inputBlock.isNull(position)) {
                crc.update(NULL_TAG);
            } else {
                long hash = inputBlock.hashCodeUseXxhash(position);

                for (int j = 0; (j * 8) < 64; j++) {
                    bytes[j] = (byte) (hash >>> (j << 3));
                }
                crc.update(bytes, 8);
            }
            crc.update(SEPARATOR_TAG);
        }
        long crcResult = crc.getValue();
        HyperLogLogUtil.hllSet(hll, crcResult);
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
