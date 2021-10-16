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
import com.alibaba.polardbx.executor.operator.MaterializedSemiJoinExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

public class MaterializedJoinExecFactory extends ExecutorFactory {

    private MaterializedSemiJoin join;
    private List<EquiJoinKey> joinKeys;
    private int parallelism;
    private List<Executor> executors = new ArrayList<>();

    public MaterializedJoinExecFactory(MaterializedSemiJoin join, ExecutorFactory innerFactory,
                                       ExecutorFactory executorFactory, int parallelism) {
        this.join = join;
        this.parallelism = parallelism;
        addInput(innerFactory);
        addInput(executorFactory);
        this.joinKeys = EquiJoinUtils
            .buildEquiJoinKeys(join, join.getOuter(), join.getInner(), (RexCall) join.getCondition(),
                join.getJoinType());

    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutor(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutor(context);
    }

    private synchronized List<Executor> createAllExecutor(ExecutionContext context) {
        if (executors.isEmpty()) {
            for (int i = 0; i < parallelism; i++) {
                Executor inner = getInputs().get(0).createExecutor(context, i);
                Executor outer = getInputs().get(1).createExecutor(context, i);
                IExpression condition = convertExpression(join.getCondition(), context);
                Executor exec = new MaterializedSemiJoinExec(
                    outer, inner, join.isDistinctInput(), joinKeys, join.getJoinType(), condition, context);
                exec.setId(join.getRelatedId());
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(join, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }
}
