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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public final class SequenceChunkBuilder {

    private SequenceChunkBuilder() {
    }

    public static Chunk createSequenceChunk(List<? extends DataType> types, int length) {
        return createSequenceChunk(types, length, new int[types.size()]);
    }

    public static Chunk createSequenceChunk(List<? extends DataType> types, int length, int... initialValues) {
        Block[] blocks = new Block[initialValues.length];
        for (int i = 0; i < blocks.length; i++) {
            DataType type = types.get(i);
            int initialValue = initialValues[i];
            Class clazz = type.getDataClass();
            if (clazz == Long.class) {
                blocks[i] = BlockAssertions.createLongSequenceBlock(initialValue, initialValue + length);
            } else if (clazz == Double.class) {
                blocks[i] = BlockAssertions.createDoubleSequenceBlock(initialValue, initialValue + length);
            } else if (clazz == String.class) {
                blocks[i] = BlockAssertions.createStringSequenceBlock(initialValue, initialValue + length);
            } else if (clazz == BooleanConfigParam.class) {
                blocks[i] = BlockAssertions.createBooleanSequenceBlock(initialValue, initialValue + length);
            } else if (clazz == Date.class) {
                blocks[i] = BlockAssertions.createDateSequenceBlock(initialValue, initialValue + length);
            } else if (clazz == Timestamp.class) {
                blocks[i] = BlockAssertions.createTimestampSequenceBlock(initialValue, initialValue + length);
            } else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Chunk(blocks);
    }
}