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

import com.google.common.collect.Lists;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Test;

import java.util.List;

public class MergeSortExecTest extends BaseExecTest {

    @Test
    public void testIntegerMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .build();

        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        OrderByOption orderByOption = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 0, 4, context);

        assertExecResults(exec, new Chunk(IntegerBlock.of(null, 1, 2, 3), IntegerBlock.of(3, 3, 4, 9)));
    }

    @Test
    public void testIntegerNegativeMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, -1, 4, 5, 9), IntegerBlock.of(3, null, 3, 4, 9)))
            .build();

        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3), IntegerBlock.of(3, 4, 9)))
            .build();

        OrderByOption orderByOption = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 0, 4, context);

        assertExecResults(exec, new Chunk(IntegerBlock.of(null, -1, 1, 2), IntegerBlock.of(3, null, 3, 4)));
    }

    @Test
    public void testInteger2ColMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), IntegerBlock.of(9, 4, 3, 2)))
            .build();

        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(9, 4, 3)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 0, 8, context);

        assertExecResults(exec,
            new Chunk(IntegerBlock.of(3, null, 8, 6, 9, 1, 5), IntegerBlock.of(9, 9, 4, 4, 3, 3, 2)));
    }

    @Test
    public void testInteger2ColWithDiffDirectionsMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), IntegerBlock.of(9, 4, 3, 2)))
            .build();

        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(9, 4, 3)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 0, 8, context);

        assertExecResults(exec,
            new Chunk(IntegerBlock.of(null, 3, 6, 8, 1, 9, 5), IntegerBlock.of(9, 9, 4, 4, 3, 3, 2)));
    }

    @Test
    public void testInteger2ColWithDiffDirectionsAnd4InputsMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), IntegerBlock.of(9, 4, 3, 2)))
            .build();
        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(9, 4, 3)))
            .build();
        MockExec input3 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 6, 15), IntegerBlock.of(null, null, null)))
            .build();
        MockExec input4 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, null), IntegerBlock.of(96, 42, 33)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);
        inputs.add(input3);
        inputs.add(input4);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 0, 8, context);

        assertExecResults(exec,
            new Chunk(IntegerBlock.of(3, 6, null, null, 3, 6, 8, 1), IntegerBlock.of(96, 42, 33, 9, 9, 4, 4, 3)));
    }

    @Test
    public void testIntegerMixString2ColWithDiffDirectionsAnd4InputsMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), StringBlock.of("9", "4", "3", "2")))
            .build();
        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), StringBlock.of("9", "4", "3")))
            .build();
        MockExec input3 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 6, 15), StringBlock.of(null, null, null)))
            .build();
        MockExec input4 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, null), StringBlock.of("96", "42", "33")))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);
        inputs.add(input3);
        inputs.add(input4);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 0, 8, context);

        assertExecResults(exec,
            new Chunk(IntegerBlock.of(3, null, 3, 6, 6, 8, null, 1),
                StringBlock.of("96", "9", "9", "42", "4", "4", "33", "3")));
    }

    @Test
    public void testInteger2ColWithDiffDirectionsAnd4InputsAndSkipMergeSort() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 8, 9, 5), IntegerBlock.of(9, 4, 3, 2)))
            .build();
        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, 1), IntegerBlock.of(9, 4, 3)))
            .build();
        MockExec input3 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 6, 15), IntegerBlock.of(null, null, null)))
            .build();
        MockExec input4 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(3, 6, null), IntegerBlock.of(96, 42, 33)))
            .build();

        OrderByOption orderByOption1 = new OrderByOption(1,
            RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);
        OrderByOption orderByOption2 = new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.UNSPECIFIED);

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);
        inputs.add(input3);
        inputs.add(input4);

        List<OrderByOption> orderByOptions = Lists.newArrayList();
        orderByOptions.add(orderByOption1);
        orderByOptions.add(orderByOption2);

        MergeSortExec exec = new MergeSortExec(inputs, orderByOptions, 4, 3, context);

        assertExecResults(exec, new Chunk(IntegerBlock.of(3, 6, 8), IntegerBlock.of(9, 4, 4)));
    }
}
