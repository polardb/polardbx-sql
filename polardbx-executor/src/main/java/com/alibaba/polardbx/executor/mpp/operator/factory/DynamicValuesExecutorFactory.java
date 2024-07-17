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

import com.alibaba.polardbx.executor.operator.DynamicValueExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.DynamicValues;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DynamicValuesExecutorFactory extends ExecutorFactory {

    private DynamicValues dynamicValues;

    public DynamicValuesExecutorFactory(DynamicValues dynamicValues) {
        this.dynamicValues = dynamicValues;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        List<DataType> outputColumns = CalciteUtils.getTypes(dynamicValues.getRowType());
        List<DynamicParamExpression> dynamicExpressions = new ArrayList<>();
        List<List<IExpression>> expressions = dynamicValues.getTuples()
            .stream().map(expression -> expression.stream().map(
                e -> RexUtils.buildRexNode(e, context, dynamicExpressions)
            ).collect(Collectors.toList())).collect(Collectors.toList());
        Executor exec = new DynamicValueExec(expressions, outputColumns, context);
        registerRuntimeStat(exec, dynamicValues, context);
        return exec;
    }

}
