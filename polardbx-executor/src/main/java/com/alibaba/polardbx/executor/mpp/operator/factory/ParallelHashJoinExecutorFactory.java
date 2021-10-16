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
import com.alibaba.polardbx.executor.operator.ParallelHashJoinExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ParallelHashJoinExecutorFactory extends ExecutorFactory {

    private boolean driverBuilder;
    private Join join;
    private List<Executor> executors = new ArrayList<>();
    private RexNode otherCond;
    private boolean maxOneRow;
    private List<RexNode> operands;
    private RexNode equalCond;
    private int probeParallelism;
    private int numPartitions;
    private boolean streamJoin;

    public ParallelHashJoinExecutorFactory(Join join, RexNode otherCond, RexNode equalCond, boolean maxOneRow,
                                           List<RexNode> operands, ExecutorFactory build, ExecutorFactory probe,
                                           int probeParallelism, int numPartitions, boolean driverBuilder) {
        this.join = join;
        this.otherCond = otherCond;
        this.equalCond = equalCond;
        this.maxOneRow = maxOneRow;
        this.operands = operands;
        this.probeParallelism = probeParallelism;
        this.numPartitions = numPartitions;
        addInput(build);
        addInput(probe);
        this.driverBuilder = driverBuilder;
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
            boolean alreadyUseRuntimeFilter = false;
            if (join instanceof HashJoin) {
                alreadyUseRuntimeFilter = ((HashJoin) join).isRuntimeFilterPushedDown();
            }
            ParallelHashJoinExec.Synchronizer synchronizer =
                new ParallelHashJoinExec.Synchronizer(numPartitions, alreadyUseRuntimeFilter);
            for (int i = 0; i < probeParallelism; i++) {
                Executor inner;
                Executor outerInput;
                if (driverBuilder) {
                    outerInput = getInputs().get(0).createExecutor(context, i);
                    inner = getInputs().get(1).createExecutor(context, i);
                } else {
                    inner = getInputs().get(0).createExecutor(context, i);
                    outerInput = getInputs().get(1).createExecutor(context, i);
                }
                IExpression otherCondition = convertExpression(otherCond, context);

                List<EquiJoinKey> joinKeys = EquiJoinUtils
                    .buildEquiJoinKeys(join, join.getOuter(), join.getInner(), (RexCall) equalCond, join.getJoinType());
                List<IExpression> antiJoinOperands = null;
                if (operands != null && join.getJoinType() == JoinRelType.ANTI && !operands.isEmpty()) {
                    antiJoinOperands =
                        operands.stream().map(ele -> convertExpression(ele, context)).collect(Collectors.toList());
                }
                ParallelHashJoinExec exec =
                    new ParallelHashJoinExec(synchronizer, outerInput, inner, join.getJoinType(), maxOneRow,
                        joinKeys, otherCondition, antiJoinOperands, driverBuilder, context, i);
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
