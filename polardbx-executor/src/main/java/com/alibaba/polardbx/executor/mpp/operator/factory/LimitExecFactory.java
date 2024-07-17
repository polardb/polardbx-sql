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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

public class LimitExecFactory extends ExecutorFactory {

    private Limit limit;
    private int parallelism;

    public LimitExecFactory(Limit limit, ExecutorFactory executorFactory, int parallelism) {
        this.limit = limit;
        this.parallelism = parallelism;
        addInput(executorFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        List<Executor> inputs = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            inputs.add(getInputs().get(0).createExecutor(context, i));
        }
        long fetch = Long.MAX_VALUE;
        long skip = 0;
        Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
        if (limit.fetch != null) {
            fetch = getRexParam(limit.fetch, params);
            if (limit.offset != null) {
                skip = getRexParam(limit.offset, params);
            }
        }
        Executor exec = new LimitExec(inputs, skip, fetch, context);
        registerRuntimeStat(exec, limit, context);
        return exec;
    }

}
