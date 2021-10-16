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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.optimizer.datastruct.ObjectSegmentArrayList;

/**
 * Created by chuanqin on 18/1/22.
 */
public class SingleValue extends AbstractAggregator {
    private BooleanSegmentArrayList valueHasAssigned;
    private ObjectSegmentArrayList values;

    public SingleValue(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, false, null, null, filterArg);
    }

    @Override
    public void open(int capacity) {
        values = new ObjectSegmentArrayList(capacity);
        valueHasAssigned = new BooleanSegmentArrayList(capacity);
    }

    @Override
    public void appendInitValue() {
        valueHasAssigned.add(false);
        values.add(null);
    }

    @Override
    public void resetToInitValue(int groupId) {
        valueHasAssigned.set(groupId, false);
        values.set(groupId, null);
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        if (!valueHasAssigned.get(groupId)) {
            valueHasAssigned.set(groupId, true);
            values.set(groupId, chunk.getBlock(aggIndexInChunk[0]).getObject(position));
        } else {
            if (ConfigDataMode.isFastMock()) {
                return;
            }
            GeneralUtil.nestedException("Subquery returns more than 1 row");
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeObject(values.get(groupId));
    }
}
