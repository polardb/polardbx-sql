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
import com.alibaba.polardbx.executor.operator.NestedLoopJoinExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LoopJoinExecutorFactory extends ExecutorFactory {

    private Join join;
    private List<Executor> executors = new ArrayList<>();
    private RexNode otherCond;
    private boolean maxOneRow;
    private List<RexNode> operands;
    private RexNode antiCondition;
    private int probeParallelism;
    private boolean streamJoin;

    public LoopJoinExecutorFactory(Join join, RexNode otherCond, boolean maxOneRow, List<RexNode> operands,
                                   RexNode antiCondition, ExecutorFactory build, ExecutorFactory probe,
                                   int probeParallelism) {
        this.join = join;
        this.otherCond = otherCond;
        this.maxOneRow = maxOneRow;
        this.operands = operands;
        this.antiCondition = antiCondition;
        this.probeParallelism = probeParallelism;
        addInput(build);
        addInput(probe);
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
            NestedLoopJoinExec.Synchronizer synchronizer = new NestedLoopJoinExec.Synchronizer();
            for (int i = 0; i < probeParallelism; i++) {
                final Executor inner = getInputs().get(0).createExecutor(context, i);
                final Executor outerInput = getInputs().get(1).createExecutor(context, i);
                IExpression otherCondition = convertExpression(otherCond, context);
                List<IExpression> antiJoinOperands = null;
                if (operands != null && join.getJoinType() == JoinRelType.ANTI && !operands.isEmpty()) {
                    antiJoinOperands =
                        operands.stream().map(ele -> convertExpression(ele, context)).collect(Collectors.toList());
                }
                NestedLoopJoinExec exec = new NestedLoopJoinExec(outerInput, inner, join.getJoinType(),
                    maxOneRow, otherCondition, antiJoinOperands, convertExpression(antiCondition, context), context,
                    synchronizer
                );
                exec.setStreamJoin(streamJoin);
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

    public void enableStreamJoin(boolean streamJoin) {
        this.streamJoin = streamJoin;
    }
}
