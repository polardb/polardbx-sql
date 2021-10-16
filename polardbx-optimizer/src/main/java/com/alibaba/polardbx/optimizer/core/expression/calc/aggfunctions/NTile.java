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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;

/**
 * Return the N th value
 *
 * @author hongxi.chx
 */
public class NTile extends AbstractAggregator {
    private long tile;
    private long count;
    private long currentPosition = 0;
    private Long largerBucketNum = null;
    private long elementInLargerBucket = 0;
    private long elementInNormalBucket = 0;

    public NTile(long tile, int filterArg) {
        super(new int[0], false, null, DataTypes.LongType, filterArg);
        this.tile = tile;
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        count++;
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        currentPosition++;
        if (largerBucketNum == null) {
            largerBucketNum = count - tile * (count / tile);
            elementInLargerBucket = largerBucketNum == 0 ? count / tile : count / tile + 1;
            elementInNormalBucket = count / tile;
        }
        if ((currentPosition - 1) < largerBucketNum * elementInLargerBucket) {
            bb.writeLong((currentPosition - 1) / elementInLargerBucket + 1);
        } else {
            long restInNormalBucket = currentPosition - 1 - (largerBucketNum * elementInLargerBucket);
            bb.writeLong(restInNormalBucket / elementInNormalBucket + largerBucketNum + 1);
        }
    }

    @Override
    public void resetToInitValue(int groupId) {
        count = 0L;
        currentPosition = 0;
        largerBucketNum = null;
        elementInLargerBucket = 0;
        elementInNormalBucket = 0;
    }
}
