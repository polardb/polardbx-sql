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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.FilterExec;
import com.alibaba.polardbx.executor.operator.VectorizedFilterExec;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterConsume;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author bairui.lrj
 */
public class FilterExecFactory extends ExecutorFactory {

    private final Filter filter;
    private final RexNode filterCondition;
    private final boolean canConvertToVectorizedExpression;

    // Staging area, should be cleared each time executor is created.
    private final boolean enableRuntimeFilter;
    private final Map<Integer, BloomFilterExpression> bloomFilterExpressionMap;

    public FilterExecFactory(Filter filter, RexNode filterCondition, ExecutorFactory executorFactory,
                             boolean canConvertToVectorizedExpression,
                             boolean enableRuntimeFilter,
                             Map<Integer, BloomFilterExpression> bloomFilterExpressionMap) {
        this.filter = filter;
        this.filterCondition = filterCondition;
        this.enableRuntimeFilter = enableRuntimeFilter;
        addInput(executorFactory);
        this.canConvertToVectorizedExpression = canConvertToVectorizedExpression;
        this.bloomFilterExpressionMap = bloomFilterExpressionMap;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        if (canConvertToVectorizedExpression) {
            return createVectorizedFilter(context, index);
        } else {
            return createRowBaseFilter(context, index);
        }
    }

    private IExpression findFilterExpressions(Filter filter, RexNode filterCondition,
                                              List<RexCall> bloomFilterNodes,
                                              List<DynamicParamExpression> dynamicExpressions,
                                              ExecutionContext context) {
        if (enableRuntimeFilter) {
            List<RexNode> conditions = RelOptUtil.conjunctions(filterCondition);
            List<RexNode> remainNodes = new ArrayList<>();
            for (RexNode condition : conditions) {
                boolean remain = true;
                if (condition instanceof RexCall) {
                    if ((((RexCall) condition).getOperator()) instanceof SqlRuntimeFilterFunction) {
                        bloomFilterNodes.add((RexCall) condition);
                        remain = false;
                    }
                }
                if (remain) {
                    remainNodes.add(condition);
                }
            }
            if (remainNodes.size() > 0) {
                return RexUtils.buildRexNode(
                    RexUtil.composeConjunction(
                        filter.getCluster().getRexBuilder(), remainNodes, true), context, dynamicExpressions);
            }
        } else {
            return RexUtils.buildRexNode(filterCondition, context, dynamicExpressions);
        }
        return null;
    }

    private Executor createRowBaseFilter(ExecutionContext context, int index) {
        List<DynamicParamExpression> dynamicExpressions = new ArrayList<>();
        List<RexCall> bloomFilterNodes = new ArrayList<>();
        IExpression expression = findFilterExpressions(
            filter, filterCondition, bloomFilterNodes, dynamicExpressions, context);
        Executor input = getInputs().get(0).createExecutor(context, index);
        if (bloomFilterNodes != null && bloomFilterNodes.size() > 0) {
            if (bloomFilterExpressionMap != null && !bloomFilterExpressionMap.containsKey(filter.getRelatedId())) {
                List<BloomFilterConsume> bloomFilters = new ArrayList<>();
                for (RexCall rexNode : bloomFilterNodes) {
                    SqlRuntimeFilterFunction runtimeFilterFunction =
                        (SqlRuntimeFilterFunction) rexNode.getOperator();
                    List<Integer> keys = new ArrayList<>();
                    for (RexNode operand : rexNode.getOperands()) {
                        keys.add(((RexSlot) operand).getIndex());
                    }
                    bloomFilters.add(new BloomFilterConsume(keys, runtimeFilterFunction.getId()));
                }
                if (bloomFilters.size() > 0) {
                    bloomFilterExpressionMap.put(filter.getRelatedId(), new BloomFilterExpression(
                        bloomFilters, false));
                }
            }
        }

        Executor exec;
        BloomFilterExpression bloomFilterExpression = null;
        if (bloomFilterExpressionMap != null) {
            bloomFilterExpression = bloomFilterExpressionMap.get(filter.getRelatedId());
        }

        exec = new FilterExec(input, expression, bloomFilterExpression, context);
        registerRuntimeStat(exec, filter, context);
        return exec;
    }

    private Executor createVectorizedFilter(ExecutionContext context, int index) {
        // TODO: Supports bloom filter in vectorized filter
        Executor inputExec = getInputs().get(0).createExecutor(context, index);

        @SuppressWarnings("unchecked")
        List<DataType<?>> inputTypes = inputExec.getDataTypes().stream()
            .map(e -> (DataType<?>) e)
            .collect(Collectors.toList());
        Pair<VectorizedExpression, MutableChunk> result =
            VectorizedExpressionBuilder.buildVectorizedExpression(inputTypes, filterCondition, context, true);

        Executor exec = new VectorizedFilterExec(inputExec, result.getKey(), result.getValue(), context);
        registerRuntimeStat(exec, filter, context);
        return exec;
    }
}
