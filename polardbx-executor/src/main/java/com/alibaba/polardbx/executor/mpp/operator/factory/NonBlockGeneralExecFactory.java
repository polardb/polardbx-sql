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

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.NonBlockGeneralSourceExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

public class NonBlockGeneralExecFactory extends ExecutorFactory {

    private RelNode relNode;

    public NonBlockGeneralExecFactory(RelNode relNode) {
        this.relNode = relNode;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Preconditions.checkArgument(index < 1, "NonBlockGeneralSourceExec's parallism must be 1!");
        return new NonBlockGeneralSourceExec(relNode, context);
    }
}
