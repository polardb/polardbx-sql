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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;

import java.util.ArrayList;
import java.util.List;

public abstract class ExecutorFactory {

    protected Exchange exchange = null;
    protected List<ExecutorFactory> childs = new ArrayList<>();

    public void setExchange(Exchange exchange) {
        this.exchange = exchange;
    }

    public List<ExecutorFactory> getInputs() {
        return childs;
    }

    public void addInput(ExecutorFactory executorFactory) {
        childs.add(executorFactory);
    }

    public abstract Executor createExecutor(ExecutionContext context, int index);

    public List<Executor> getAllExecutors(ExecutionContext context) {
        throw new UnsupportedOperationException();
    }

    public void explain(StringBuilder output) {
        output.append(this.getClass().getSimpleName() + "(");
        for (int i = 0; i < childs.size(); i++) {
            if (i != 0) {
                output.append(",");
            }
            output.append(childs.get(i).getClass().getSimpleName());
        }
        output.append(")");
    }

    protected void registerRuntimeStat(Executor exec, RelNode relNode, ExecutionContext context) {
        exec.setId(relNode.getRelatedId());
        if (context.getRuntimeStatistics() != null) {
            if (exchange != null) {
                RuntimeStatHelper.registerStatForExec(exchange, exec, context);
            }
            RuntimeStatHelper.registerStatForExec(relNode, exec, context);
        }
    }
}
