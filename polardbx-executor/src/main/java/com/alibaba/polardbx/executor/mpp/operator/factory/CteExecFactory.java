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
import com.alibaba.polardbx.executor.operator.LimitExec;
import com.alibaba.polardbx.executor.operator.RecursiveCTEExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.RecursiveCTE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

/**
 * @author jilong.ljl
 */
public class CteExecFactory extends ExecutorFactory {

    private final RecursiveCTE cte;
    private ExecutorFactory anchorExecutorFactory;
    private ExecutorFactory recursiveExecutorFactory;

    public CteExecFactory(RecursiveCTE cte,
                          ExecutorFactory anchorExecutorFactory,
                          ExecutorFactory recursiveExecutorFactory) {
        this.cte = cte;
        this.anchorExecutorFactory = anchorExecutorFactory;
        this.recursiveExecutorFactory = recursiveExecutorFactory;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        long fetch = Long.MAX_VALUE;
        long skip = 0;

        Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
        if (cte.getFetch() != null) {
            fetch = getRexParam(cte.getFetch(), params);
            if (cte.getOffset() != null) {
                skip = getRexParam(cte.getOffset(), params);
            }
        }
        long fetchSize = skip + fetch;
        if (skip > 0 && fetch > 0 && fetchSize < 0) {
            fetchSize = Long.MAX_VALUE;
        }

        Executor anchorExecutor = anchorExecutorFactory.createExecutor(context, index);
        RecursiveCTEExec recursiveCTEExec =
            new RecursiveCTEExec(cte.getCteName(), anchorExecutor, recursiveExecutorFactory, fetchSize, context);
        recursiveCTEExec.setId(cte.getRelatedId());
        if (context.getRuntimeStatistics() != null) {
            RuntimeStatHelper.registerStatForExec(cte, recursiveCTEExec, context);
        }
        if (cte.getFetch() == null && cte.getOffset() == null) {
            return recursiveCTEExec;
        }
        List<Executor> inputs = new ArrayList<>();
        inputs.add(recursiveCTEExec);
        Executor exec = new LimitExec(inputs, skip, fetch, context);

        return exec;
    }

}
