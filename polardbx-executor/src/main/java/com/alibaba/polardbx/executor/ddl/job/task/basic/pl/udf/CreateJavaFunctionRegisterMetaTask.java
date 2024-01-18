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
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.JavaFunctionAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateJavaFunctionRegisterMetaTask")
public class CreateJavaFunctionRegisterMetaTask extends BaseGmsTask {
    private final String functionName;
    protected final String javaCode;

    protected final String returnType;

    protected final String inputTypes;

    protected final Boolean noState;

    @JSONCreator
    public CreateJavaFunctionRegisterMetaTask(String schemaName, String logicalTableName,
                                              String functionName, String javaCode,
                                              String returnType, String inputTypes, Boolean noState) {
        super(schemaName, logicalTableName);
        this.functionName = functionName;
        this.javaCode = javaCode;
        this.inputTypes = inputTypes;
        this.returnType = returnType;
        this.noState = noState;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        JavaFunctionAccessor accessor = new JavaFunctionAccessor();
        accessor.setConnection(metaDbConnection);
        accessor.insertFunction(functionName, StringUtils.funcNameToClassName(functionName), javaCode, inputTypes,
            returnType, noState);

    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        JavaFunctionAccessor accessor = new JavaFunctionAccessor();
        accessor.setConnection(metaDbConnection);
        accessor.dropFunction(functionName);
    }
}
