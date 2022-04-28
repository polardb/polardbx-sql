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
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableDecimalLongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DEFAULT_DIV_PRECISION_INCREMENT;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DIV_ZERO;

public abstract class SpecificType2DecimalAvg extends AbstractAggregator {

    private NullableDecimalLongGroupState state;

    private Decimal cache;

    /**
     * get div_precision_increment user variables from session.
     */
    private int divPrecisionIncr;

    public SpecificType2DecimalAvg(int index, boolean isDistict, DataType inputType, DataType outputType,
                                   int filterArg) {
        super(new int[] {index}, isDistict, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        this.state = new NullableDecimalLongGroupState(capacity);
        this.divPrecisionIncr = DEFAULT_DIV_PRECISION_INCREMENT;
        this.cache = new Decimal();
    }

    @Override
    public void appendInitValue() {
        state.appendNull();
    }

    @Override
    public void resetToInitValue(int groupId) {
        state.setNull(groupId);
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        Block block = chunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        final Decimal value = getDecimal(block, position);
        if (state.isNull(groupId)) {
            state.set(groupId, value.copy(), 1);
        } else {
            Decimal before = state.getDecimal(groupId);

            // avoid to allocate memory
            before.add(value, cache);
            Decimal sum = cache;
            cache = before;
            long count = state.getLong(groupId) + 1;
            state.set(groupId, sum, count);
        }
    }

    abstract Decimal getDecimal(Block block, int position);

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (state.isNull(groupId)) {
            bb.appendNull();
        } else {
            DecimalStructure rounded = new DecimalStructure();
            DecimalStructure unRounded = new DecimalStructure();

            // fetch sum & count decimal value
            Decimal sum = state.getDecimal(groupId);
            Decimal count = Decimal.fromLong(state.getLong(groupId));

            // do divide
            int error = FastDecimalUtils
                .div(sum.getDecimalStructure(), count.getDecimalStructure(), unRounded, divPrecisionIncr);
            if (error == E_DEC_DIV_ZERO) {
                // divide zero, set null
                bb.appendNull();
            } else {
                // do round
                FastDecimalUtils.round(unRounded, rounded, divPrecisionIncr, DecimalRoundMod.HALF_UP);
                Decimal avg = new Decimal(rounded);
                bb.writeDecimal(avg);
            }
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}


