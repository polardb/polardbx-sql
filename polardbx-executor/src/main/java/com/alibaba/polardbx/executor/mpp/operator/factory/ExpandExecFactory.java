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

import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ExpandExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

public class ExpandExecFactory extends ExecutorFactory {

    private LogicalExpand expand;
    private List<DataType> outputColumns;

    public ExpandExecFactory(LogicalExpand expand, ExecutorFactory executorFactory) {
        this.expand = expand;
        addInput(executorFactory);
        this.outputColumns = CalciteUtils.getTypes(expand.getRowType());
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        List<List<RexNode>> expandExprs = expand.getProjects();
        List<List<IExpression>> exprsList = new ArrayList<>(expandExprs.size());
        for (List<RexNode> rexNodes : expandExprs) {
            List<IExpression> exprs = new ArrayList<>();
            for (RexNode rexNode : rexNodes) {
                exprs.add(RexUtils.buildRexNode(rexNode, context));
            }
            exprsList.add(exprs);
        }

        Executor input = getInputs().get(0).createExecutor(context, index);
        Executor exec = new ExpandExec(input, exprsList, outputColumns, context);
        registerRuntimeStat(exec, expand, context);
        return exec;
    }

}
