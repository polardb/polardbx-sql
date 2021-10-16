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

package com.alibaba.polardbx.optimizer.core.sequence.bean;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_GROUP_TABLE_NAME;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_NAME_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_VALUE_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_INNER_STEP_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_UNIT_COUNT_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_UNIT_INDEX_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;

public class CreateCustomUnitGroupSequence extends CreateGroupSequence {

    public CreateCustomUnitGroupSequence(String name, String schema) {
        super(name);
        setSchemaName(schema);
    }

    @Override
    public String getSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(DEFAULT_GROUP_TABLE_NAME).append("(");
        sb.append(DEFAULT_NAME_COLUMN).append(", ");
        sb.append(DEFAULT_VALUE_COLUMN).append(", ");
        sb.append(EXT_UNIT_COUNT_COLUMN).append(", ");
        sb.append(EXT_UNIT_INDEX_COLUMN).append(", ");
        sb.append(EXT_INNER_STEP_COLUMN).append(", ");
        sb.append(SequenceAttribute.DEFAULT_GMT_MODIFIED_COLUMN).append(") ");
        sb.append("VALUES('%s', %s, %s, %s, %s, NOW())");

        // 3 arguments from diamond or default: unit_count and inner_step
        int[] unitArgs = SequenceManagerProxy.getInstance().getCustomUnitArgsForGroupSeq(getSchemaName());

        // Priority of global/app properties and command arguments:
        // --------------------------------------------------------
        // if command is specified
        // command
        // else if global is specified
        // global
        // else
        // default
        // --------------------------------------------------------
        if (unitCount <= UNDEFINED_UNIT_COUNT || unitCount > UPPER_LIMIT_UNIT_COUNT) {
            // unitCount is invalid
            if (unitArgs[0] > UNDEFINED_UNIT_COUNT && unitArgs[0] <= UPPER_LIMIT_UNIT_COUNT) {
                unitCount = unitArgs[0];
            } else {
                unitCount = DEFAULT_UNIT_COUNT;
            }
        }
        if (unitIndex <= UNDEFINED_UNIT_INDEX || unitIndex >= unitCount) {
            if (unitArgs[1] > UNDEFINED_UNIT_INDEX || unitArgs[1] < unitCount) {
                unitIndex = unitArgs[1];
            } else {
                unitIndex = DEFAULT_UNIT_INDEX;
            }
        }
        if (innerStep <= UNDEFINED_INNER_STEP) {
            // innerStep is invalid
            if (unitArgs[2] > UNDEFINED_INNER_STEP) {
                innerStep = unitArgs[2];
            } else {
                innerStep = DEFAULT_INNER_STEP;
            }
        }

        // Recalculate start based on unit_index and inner_step
        start = start > 1 ? start : unitIndex > 0 ? unitIndex * innerStep : 0;

        return String.format(sb.toString(),
            name,
            String.valueOf(start),
            String.valueOf(unitCount),
            String.valueOf(unitIndex),
            String.valueOf(innerStep));
    }

}
