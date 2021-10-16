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
import com.alibaba.polardbx.executor.operator.ProjectExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.core.Union;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Union 是逻辑节点，所以对应UnionExecFactory也只是逻辑概念
 */
public class UnionExecFactory extends ExecutorFactory {

    private List<ExecutorFactory> inputs;
    private List<Integer> parallelisms;
    private List<List<IExpression>> converts;
    private List<DataType> outputColumns;

    public UnionExecFactory(Union union, List<ExecutorFactory> inputs, List<Integer> parallelisms) {
        this.inputs = inputs;
        this.parallelisms = parallelisms;
        List<DataType> targetTypes = CalciteUtils.getTypes(union.getRowType());
        this.converts = new ArrayList<>();
        for (int i = 0; i < union.getInputs().size(); i++) {
            List<DataType> types = CalciteUtils.getTypes(union.getInput(i).getRowType());
            converts.add(createCastExpressions(types, targetTypes));
        }
        this.outputColumns = CalciteUtils.getTypes(union.getRowType());
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {

        int targetIndex = 0;
        for (int i = 0; i < parallelisms.size(); i++) {
            targetIndex = i;
            if (index < parallelisms.get(i)) {
                break;
            } else {
                index -= parallelisms.get(i);
            }
        }

        Executor ret = inputs.get(targetIndex).createExecutor(context, index);
        if (converts.get(targetIndex) != null) {
            ret = new ProjectExec(ret, converts.get(targetIndex), outputColumns, context);
        }
        return ret;
    }

    private List<IExpression> createCastExpressions(List<DataType> sources, List<DataType> targets) {
        if (!sources.equals(targets)) {
            List<IExpression> converts = new ArrayList<>();
            for (int i = 0; i < targets.size(); i++) {
                if (targets.get(i) != sources.get(i)) {
                    converts.add(new CastExpression(targets.get(i), i));
                } else {
                    converts.add(new InputRefExpression(i));
                }
            }
            return converts;
        }
        return null;
    }

    public static class CastExpression implements IExpression {
        private DataType toType;
        private int sourceIndex;

        public CastExpression(DataType toType, int sourceIndex) {
            this.toType = toType;
            this.sourceIndex = sourceIndex;
        }

        @Override
        public Object eval(Row row) {
            return toType.convertFrom(row.getObject(sourceIndex));
        }

        @Override
        public Object eval(Row row, ExecutionContext ec) {
            return eval(row);
        }

        @Override
        public Object evalEndPoint(Row row, ExecutionContext ec, Boolean cmpDirection, AtomicBoolean inclEndp) {
            return eval(row);
        }
    }

}
