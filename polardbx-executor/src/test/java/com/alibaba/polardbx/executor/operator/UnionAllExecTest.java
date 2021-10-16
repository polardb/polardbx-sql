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
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class UnionAllExecTest extends BaseExecTest {

    @Test
    public void testUnionAll_Simple() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)))
            .build();

        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3, 4), IntegerBlock.of(3, 4, 9, 3)))
            .build();

        List<OrderByOption> orderBys = new ArrayList<>(2);
        orderBys.add(new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));
        orderBys.add(new OrderByOption(1,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);

        UnionAllExec exec = new UnionAllExec(inputs, input1.getDataTypes(), context);

        assertExecResults(exec,
            new Chunk(IntegerBlock.of(null, 4, 5, 9), IntegerBlock.of(3, 3, 4, 9)),
            new Chunk(IntegerBlock.of(1, 2, 3, 4), IntegerBlock.of(3, 4, 9, 3)));
    }

    @Test
    public void testUnionAll_MulDup() {
        MockExec input1 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(null, 4, null, 5, 9, null),
                IntegerBlock.of(3, 3, null, 4, 9, null)))
            .build();

        MockExec input2 = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(5, 1, 2, null, 3, 4), IntegerBlock.of(4, 3, 4, null, 9, 3)))
            .build();

        List<OrderByOption> orderBys = new ArrayList<>(2);
        orderBys.add(new OrderByOption(0,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));
        orderBys.add(new OrderByOption(1,
            RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.FIRST));

        List<Executor> inputs = Lists.newArrayList();
        inputs.add(input1);
        inputs.add(input2);

        UnionAllExec exec = new UnionAllExec(inputs, input1.getDataTypes(), context);

        assertExecResults(exec,
            new Chunk(IntegerBlock.of(null, 4, null, 5, 9, null), IntegerBlock.of(3, 3, null, 4, 9, null)),
            new Chunk(IntegerBlock.of(5, 1, 2, null, 3, 4), IntegerBlock.of(4, 3, 4, null, 9, 3)));
    }
}
