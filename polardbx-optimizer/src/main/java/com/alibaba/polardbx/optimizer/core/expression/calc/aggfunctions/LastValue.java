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
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;

/**
 * Return the last value of the window
 *
 * @author hongxi.chx
 */
public class LastValue extends AbstractAggregator {
    private Object outputValue;

    public LastValue(int index, int filterArg) {
        super(new int[] {index}, false, null, null, filterArg);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        outputValue = inputChunk.getBlock(aggIndexInChunk[0]).getObject(position);
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeObject(outputValue);
    }

    @Override
    public void resetToInitValue(int groupId) {
        outputValue = null;
    }
}
