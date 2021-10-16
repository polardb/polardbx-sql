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

public class Lead extends Lag {

    public Lead(int index, long offset, Object defaultLagValue, int filterArg) {
        super(index, offset, defaultLagValue, filterArg);
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        index++;
        if (index + offset > count) {
            if (defaultValue == null) {
                bb.appendNull();
            } else {
                bb.writeString(defaultValue.toString());
            }
        } else {
            Object value = indexToValue.get(index + offset).getObject(aggIndexInChunk[0]);
            if (value == null) {
                bb.appendNull();
            } else {
                bb.writeString(value.toString());
            }
        }
    }
}
