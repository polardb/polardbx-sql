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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.TddlJavaTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class VectorizedProjectExecTest extends BaseExecTest {
    // Inputs
    private MockExec inputExecutor;
    private List<RexNode> projections;

    // Outputs
    private List<Chunk> expectedOutputs;

    public void doProjectTest() {
        List<DataType<?>> inputDataTypes = inputExecutor.getDataTypes()
            .stream()
            .map(a -> (DataType<?>) a)
            .collect(Collectors.toList());

        List<VectorizedExpression> expressions = new ArrayList<>(projections.size());
        List<MutableChunk> chunks = new ArrayList<>(projections.size());

        for (RexNode rexNode : projections) {
            Pair<VectorizedExpression, MutableChunk> result =
                VectorizedExpressionBuilder.buildVectorizedExpression(inputDataTypes, rexNode, context);
            expressions.add(result.getKey());
            chunks.add(result.getValue());
        }

        List<DataType> columns =
            expressions.stream().map(VectorizedExpression::getOutputDataType).collect(
                Collectors.toList());
        VectorizedProjectExec projectExec =
            new VectorizedProjectExec(inputExecutor, expressions, chunks, columns, context);

        assertExecResults(projectExec, expectedOutputs.toArray(new Chunk[0]));
    }

    @Test
    public void testProjections() {
        inputExecutor = RowChunksBuilder.rowChunksBuilder(DataTypes.DecimalType, DataTypes.DecimalType)
            .row(null, Decimal.fromLong(1))
            .row(Decimal.fromLong(1), Decimal.fromLong(2))
            .row(Decimal.fromLong(2), Decimal.fromLong(3))
            .chunkBreak()
            .row(Decimal.fromLong(3), Decimal.fromLong(4))
            .buildExec();
        RelDataTypeFactory typeSystem = new TddlJavaTypeFactoryImpl();
        RexBuilder builder = new RexBuilder(typeSystem);

        RexNode input0 = builder.makeInputRef(typeSystem.createSqlType(SqlTypeName.DECIMAL), 0);
        RexNode input1 = builder.makeInputRef(typeSystem.createSqlType(SqlTypeName.DECIMAL), 1);
        RexNode input2 = builder.makeIntLiteral(5);

        RexNode add1 = builder.makeCall(typeSystem.createSqlType(SqlTypeName.DECIMAL), SqlStdOperatorTable.PLUS,
            Arrays.asList(input0, input1));
        RexNode add2 = builder.makeCall(typeSystem.createSqlType(SqlTypeName.DECIMAL), SqlStdOperatorTable.PLUS,
            Arrays.asList(input1, input2));

        projections = Lists.newArrayList(add1, add2, input0);

        expectedOutputs =
            RowChunksBuilder.rowChunksBuilder(DataTypes.DecimalType, DataTypes.DecimalType, DataTypes.DecimalType)
                .row(null, Decimal.fromLong(6), null)
                .row(Decimal.fromLong(3), Decimal.fromLong(7), Decimal.fromLong(1))
                .row(Decimal.fromLong(5), Decimal.fromLong(8), Decimal.fromLong(2))
                .chunkBreak()
                .row(Decimal.fromLong(7), Decimal.fromLong(9), Decimal.fromLong(3))
                .build();

        doProjectTest();
    }
}
