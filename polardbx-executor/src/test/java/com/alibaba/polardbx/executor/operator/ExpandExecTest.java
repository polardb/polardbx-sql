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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ExpandExecTest extends BaseExecTest {

    @Test
    public void test() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
                .row(1, 5).row(2, 6).row(3, 7).row(4, 8);
        MockExec input = rowChunksBuilder.buildExec();

        List<List<IExpression>> expressions = new ArrayList<>();
        List<IExpression> expressions1 = new ArrayList<>();
        expressions1.add(new InputRefExpression(0));
        expressions.add(expressions1);

        List<IExpression> expressions2 = new ArrayList<>();
        expressions2.add(new InputRefExpression(1));
        expressions.add(expressions2);

        List<DataType> columns = new ArrayList<>();
        columns.add(input.getDataTypes().get(0));
        ExpandExec expandExec = new ExpandExec(input, expressions, columns, context);
        List<Chunk> expects = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType)
            .row(1).row(2).row(3).row(4).row(5).row(6).row(7).row(8).build();
        execForSmpMode(expandExec, expects, true);
    }
}
