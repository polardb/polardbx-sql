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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.operator.MockExec;
import com.alibaba.polardbx.executor.operator.VectorizedProjectExec;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseProjectionTest extends BaseVectorizedExpressionTest {
    private final String sql;
    private final List<ColumnInput> inputs;
    private final Block output;
    private final int[] selection;
    private final Block selectedOutput;

    public BaseProjectionTest(String sql, List<ColumnInput> inputs, Block output, int[] selection) {
        this.sql = sql;
        this.inputs = inputs;
        this.output = output;
        this.selection = selection;
        this.selectedOutput = copySelected(output, selection);
    }

    public static String projection(String expression) {
        return String.format("select %s from `t_table`", expression);
    }

    @Test
    public void doTestWithoutSelection() {
        doTest(false);
    }

    @Test
    public void doTestWithSelection() {
        doTest(true);
    }

    private void doTest(boolean useSelection) {
        updateColumns(inputs);

        LogicalProject relNode = (LogicalProject) sql2Plan(sql);
        List<RexNode> projections = relNode.getProjects();

        MockExec inputExecutor = buildInputFromTableData(useSelection ? selection : null);
        List<DataType<?>> inputDataTypes = inputExecutor.getDataTypes()
            .stream()
            .map(a -> (DataType<?>) a)
            .collect(Collectors.toList());

        List<VectorizedExpression> expressions = new ArrayList<>(projections.size());
        List<MutableChunk> chunks = new ArrayList<>(projections.size());

        for (RexNode rexNode : projections) {
            Pair<VectorizedExpression, MutableChunk> result =
                VectorizedExpressionBuilder.buildVectorizedExpression(inputDataTypes, rexNode, getExecutionContext());
            expressions.add(result.getKey());
            chunks.add(result.getValue());
        }

        List<DataType> columns =
            expressions.stream().map(VectorizedExpression::getOutputDataType).collect(
                Collectors.toList());
        VectorizedProjectExec projectExec =
            new VectorizedProjectExec(inputExecutor, expressions, chunks, columns, getExecutionContext());

        List<Chunk> expectedOutputs;
        if (useSelection) {
            expectedOutputs = Collections.singletonList(new Chunk(selectedOutput));
        } else {
            expectedOutputs = Collections.singletonList(new Chunk(output));
        }
        BaseExecTest.assertExecResults(projectExec, expectedOutputs.toArray(new Chunk[0]));
    }
}
