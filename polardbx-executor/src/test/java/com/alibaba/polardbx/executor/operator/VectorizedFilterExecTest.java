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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VectorizedFilterExecTest extends BaseExecTest {
    // Inputs
    private MockExec inputExecutor;
    private RexNode rexNode;

    // Outputs
    private List<Chunk> expectedOutputs;

    public void doFilterTest() {
        List<DataType<?>> inputDataTypes = inputExecutor.getDataTypes()
            .stream()
            .map(a -> (DataType<?>) a)
            .collect(Collectors.toList());

        VectorizedExpression condition;
        MutableChunk chunk;

        Pair<VectorizedExpression, MutableChunk> result =
            VectorizedExpressionBuilder.buildVectorizedExpression(inputDataTypes, rexNode, context);
        condition = result.getKey();
        chunk = result.getValue();

        VectorizedFilterExec filterExec =
            new VectorizedFilterExec(inputExecutor, condition, chunk, context);

        assertExecResults(filterExec, expectedOutputs.toArray(new Chunk[0]));
    }

    @Before
    public void setup() {
        Map<String, String> config = new HashMap<>();
        config.put(ConnectionParams.CHUNK_SIZE.getName(), "2");
        context.setParamManager(new ParamManager(config));
    }

    @Test
    public void testFunctionFilter() {
        inputExecutor = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(null, 1)
            .row(1, 2)
            .chunkBreak()
            .row(5, 3)
            .row(3, 4)
            .buildExec();
        RelDataTypeFactory typeSystem = new TddlJavaTypeFactoryImpl();
        RexBuilder builder = new RexBuilder(typeSystem);

        RexNode input0 = builder.makeInputRef(typeSystem.createSqlType(SqlTypeName.INTEGER), 0);
        RexNode input1 = builder.makeInputRef(typeSystem.createSqlType(SqlTypeName.INTEGER), 1);

        rexNode = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, input0, input1);

        expectedOutputs = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(5, 3)
            .build();

        doFilterTest();
    }

    @Test
    public void testInputRefFilter() {
        inputExecutor = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(null, 1)
            .row(1, 2)
            .chunkBreak()
            .row(5, 3)
            .row(3, 4)
            .buildExec();
        RelDataTypeFactory typeSystem = new TddlJavaTypeFactoryImpl();
        RexBuilder builder = new RexBuilder(typeSystem);

        rexNode = builder.makeInputRef(typeSystem.createSqlType(SqlTypeName.INTEGER), 0);

        expectedOutputs = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(1, 2)
            .row(5, 3)
            .chunkBreak()
            .row(3, 4)
            .build();

        doFilterTest();
    }

    @Test
    public void testConstInputFilter() {
        inputExecutor = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(null, 1)
            .row(1, 2)
            .chunkBreak()
            .row(5, 3)
            .row(3, 4)
            .buildExec();
        RelDataTypeFactory typeSystem = new TddlJavaTypeFactoryImpl();
        RexBuilder builder = new RexBuilder(typeSystem);

        rexNode = builder.makeIntLiteral(10);

        expectedOutputs = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(null, 1)
            .row(1, 2)
            .chunkBreak()
            .row(5, 3)
            .row(3, 4)
            .build();

        doFilterTest();
    }

}
