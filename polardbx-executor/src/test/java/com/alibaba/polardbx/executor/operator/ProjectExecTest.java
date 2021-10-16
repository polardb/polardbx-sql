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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ProjectExecTest extends BaseExecTest {

    public ProjectExecTest() {
    }

    @Test
    public void testProject() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType)
                .row(1, 2, 3).row(2, 3, 4).row(3, 4, 5).row(4, 5, 6);
        MockExec input = rowChunksBuilder.buildExec();

        context.setParams(new Parameters());

        List<IExpression> expressions = Arrays.asList(new InputRefExpression(0), new InputRefExpression(2));
        List<DataType> columns = ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);
        ProjectExec project = new ProjectExec(input, expressions, columns, context);
        List<Chunk> expects = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(1, 3).row(2, 4).row(3, 5).row(4, 6).build();
        execForSmpMode(project, expects, false);
    }
}
