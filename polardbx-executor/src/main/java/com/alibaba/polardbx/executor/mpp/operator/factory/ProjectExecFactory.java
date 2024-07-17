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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ProjectExec;
import com.alibaba.polardbx.executor.operator.VectorizedProjectExec;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProjectExecFactory extends ExecutorFactory {

    private Project project;
    private List<RexNode> projectExprs;
    private List<DataType> outputColumns;
    private final boolean canConvertToVectorizedExpression;

    public ProjectExecFactory(Project project, List<RexNode> projectExprs, ExecutorFactory executorFactory,
                              boolean canConvertToVectorizedExpression) {
        this.project = project;
        addInput(executorFactory);
        this.projectExprs = projectExprs;
        this.outputColumns = CalciteUtils.getTypes(project.getRowType());
        this.canConvertToVectorizedExpression = canConvertToVectorizedExpression;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        if (canConvertToVectorizedExpression) {
            return buildVectorizedProjectionExecutor(context, index);
        } else {
            return createRowBasedProjectionExecutor(context, index);
        }
    }

    private Executor createRowBasedProjectionExecutor(ExecutionContext context, int index) {

        List<DynamicParamExpression> dynamicExpressions = new ArrayList<>();
        List<IExpression> expressions =
            projectExprs.stream().map(e -> RexUtils.buildRexNode(e, context, dynamicExpressions))
                .collect(Collectors.toList());

        Executor input = getInputs().get(0).createExecutor(context, index);

        Executor exec = new ProjectExec(input, expressions, outputColumns, context);
        registerRuntimeStat(exec, project, context);
        return exec;
    }

    private Executor buildVectorizedProjectionExecutor(ExecutionContext context, int index) {
        Executor inputExec = getInputs().get(0).createExecutor(context, index);

        @SuppressWarnings("unchecked")
        List<DataType<?>> inputTypes = inputExec.getDataTypes().stream()
            .map(e -> (DataType<?>) e)
            .collect(Collectors.toList());
        List<VectorizedExpression> expressions = new ArrayList<>(projectExprs.size());

        List<MutableChunk> preAllocatedChunks = new ArrayList<>(projectExprs.size());
        projectExprs
            .stream()
            .map(e -> VectorizedExpressionBuilder.buildVectorizedExpression(inputTypes, e, context))
            .forEach(pair -> {
                expressions.add(pair.getKey());
                preAllocatedChunks.add(pair.getValue());
            });

        Executor exec = new VectorizedProjectExec(inputExec, expressions, preAllocatedChunks, outputColumns, context);
        registerRuntimeStat(exec, project, context);
        return exec;
    }
}
