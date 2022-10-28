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
import com.alibaba.polardbx.executor.operator.InsertSelectExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;

/**
 * @author lijiu.lzw
 */
public class InsertSelectExecFactory extends ExecutorFactory {
    private final LogicalInsert relNode;

    public InsertSelectExecFactory(LogicalInsert relNode, ExecutorFactory inputFactory) {
        this.relNode = relNode;
        addInput(inputFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Executor inputExec = getInputs().get(0).createExecutor(context, index);

        Executor exec = new InsertSelectExec(relNode, inputExec, context);
        if (context.getRuntimeStatistics() != null) {
            RuntimeStatHelper.registerStatForExec(relNode, exec, context);
        }
        exec.setId(relNode.getRelatedId());
        return exec;
    }
}
