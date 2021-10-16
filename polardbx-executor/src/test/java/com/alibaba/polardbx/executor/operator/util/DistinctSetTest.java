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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.chunk.StringBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Eric Fu
 */
public class DistinctSetTest {

    @Test
    public void test() {
        DataType[] dataTypes = new DataType[] {DataTypes.StringType};
        DistinctSet distinctSet = new DistinctSet(dataTypes, new int[] {0}, 100, 10, new ExecutionContext());

        {
            IntegerBlock groupIdBlock = IntegerBlock.of(1, 2, 3, 4, 1, 2, 3, 4);
            Chunk valueChunk = new Chunk(StringBlock.of("foo", "bar", "foo", "bar", "bar", "bar", "bar", "bar"));
            boolean[] expect = new boolean[] {true, true, true, true, true, false, true, false};

            boolean[] actual = distinctSet.checkDistinct(groupIdBlock, valueChunk);
            assertArrayEquals(expect, actual);
        }

        {
            IntegerBlock groupIdBlock = IntegerBlock.of(1, 2, 0, 4);
            Chunk valueChunk = new Chunk(StringBlock.of("foo", "foo", "X", "bar"));
            IntList positions = IntArrayList.wrap(new int[] {0, 1, 3});
            boolean[] expect = new boolean[] {false, true, false};

            boolean[] actual = distinctSet.checkDistinct(groupIdBlock, valueChunk, positions);
            assertArrayEquals(expect, actual);
        }

    }
}
