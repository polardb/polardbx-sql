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

public class RowNumber extends AbstractAggregator {
    private long number = 0;

    public RowNumber(int filterArg) {
        super(new int[0], false, null, DataTypes.LongType, filterArg);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        number++;
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeLong(number);
    }

    @Override
    public void resetToInitValue(int groupId) {
        number = 0;
    }
}
