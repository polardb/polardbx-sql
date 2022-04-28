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
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Arrays;
import java.util.Collections;

public class Decimal2DecimalMin extends Decimal2DecimalMax {

    public Decimal2DecimalMin(int index, DataType inputType, DataType outputType, int filterArg) {
        super(index, inputType, outputType, filterArg);
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        Block block = chunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        final Decimal value = block.getDecimal(position);
        if (groupState.isNull(groupId)) {
            groupState.set(groupId, value);
        } else {
            Decimal beforeValue = groupState.get(groupId);
            Decimal afterValue = Collections.min(Arrays.asList(beforeValue, value));
            groupState.set(groupId, afterValue);
        }
    }
}

