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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.MergeSortExec;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

public class LocalMergeSortExecutorFactory extends ExecutorFactory {

    private Sort sort;
    private List<Executor> executors = new ArrayList<>();
    private int childParallism;

    public LocalMergeSortExecutorFactory(Sort sort, ExecutorFactory executorFactory, int childParallism) {
        this.sort = sort;
        this.childParallism = childParallism;
        addInput(executorFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutors(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutors(context);
    }

    private synchronized List<Executor> createAllExecutors(ExecutionContext context) {
        if (executors.isEmpty()) {
            List<Executor> inputs = new ArrayList<>();
            for (int i = 0; i < childParallism; i++) {
                inputs.add(getInputs().get(0).createExecutor(context, i));
            }
            List<RelFieldCollation> sortList = sort.getCollation().getFieldCollations();
            List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);
            long limit = Long.MAX_VALUE;
            long offset = 0;
            final Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
            if (sort.fetch != null) {
                limit = getRexParam(sort.fetch, params);
                if (sort.offset != null) {
                    offset = getRexParam(sort.offset, params);
                }
            }
            MergeSortExec sortExec = new MergeSortExec(inputs, orderBys, offset, limit, context);
            sortExec.setId(sort.getRelatedId());
            if (context.getRuntimeStatistics() != null) {
                RuntimeStatHelper.registerStatForExec(sort, sortExec, context);
            }
            executors.add(sortExec);
        }
        return executors;
    }
}
