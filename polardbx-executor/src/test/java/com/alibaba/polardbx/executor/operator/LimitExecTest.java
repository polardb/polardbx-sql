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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.junit.Test;

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Collections;

public class LimitExecTest extends BaseExecTest {

    @Test
    public void testLimitSimple() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 0, 4, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(null, 4, 5, 9),
                IntegerBlock.of(3, 3, 4, 9))),
            false);
    }

    @Test
    public void testLimit08() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 0, 8, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(
                IntegerBlock.of(null, 4, 5, 9, 1, 2, 3),
                IntegerBlock.of(3, 3, 4, 9, 3, 4, 9))), false);
    }

    @Test
    public void testLimit00() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 0, 0, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.EMPTY_LIST, false);
    }

    @Test
    public void testLimit88() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 8, 8, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.EMPTY_LIST, false);
    }

    @Test
    public void testLimit48() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 4, 8, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9))), false);
    }

    @Test
    public void testLimit33() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 3, 3, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(9, 1, 2), IntegerBlock.of(9, 3, 4))), false);
    }

    @Test
    public void testLimit38() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        LimitExec exec = new LimitExec(Lists.newArrayList(input1), 3, 8, context);
        SingleExecTest test = new SingleExecTest.Builder(exec).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(
            new Chunk(IntegerBlock.of(9, 1, 2, 3), IntegerBlock.of(9, 3, 4, 9))), false);
    }
}
