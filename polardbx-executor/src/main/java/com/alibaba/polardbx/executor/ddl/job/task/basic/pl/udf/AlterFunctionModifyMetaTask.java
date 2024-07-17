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

package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AlterFunctionModifyMetaTask")
public class AlterFunctionModifyMetaTask extends BaseGmsTask {
    protected final String functionName;
    private final String alterFunctionContent;

    @JSONCreator
    public AlterFunctionModifyMetaTask(String schemaName, String logicalTableName, String functionName,
                                       String alterFunctionContent) {
        super(schemaName, logicalTableName);
        this.functionName = functionName;
        this.alterFunctionContent = alterFunctionContent;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomSuspendFromHint(executionContext);

        FunctionAccessor functionAccessor = new FunctionAccessor();
        functionAccessor.setConnection(metaDbConnection);
        functionAccessor.alterFunction(functionName, alterFunctionContent);
    }
}
