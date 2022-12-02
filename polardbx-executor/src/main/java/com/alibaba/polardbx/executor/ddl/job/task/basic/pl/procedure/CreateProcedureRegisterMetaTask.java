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

package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.procedure;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.PlParameterAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.ProcedureAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateProcedureRegisterMetaTask")
public class CreateProcedureRegisterMetaTask extends BaseGmsTask {
    protected final String procedureName;
    protected final String procedureSchema;
    private final String createProcedureContent;

    @JSONCreator
    public CreateProcedureRegisterMetaTask(String schemaName, String logicalTableName, String procedureSchema,
                                           String procedureName, String createProcedureContent) {
        super(schemaName, logicalTableName);
        this.procedureSchema = procedureSchema;
        this.procedureName = procedureName;
        this.createProcedureContent = createProcedureContent;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomSuspendFromHint(executionContext);

        ProcedureAccessor procedureAccessor = new ProcedureAccessor();
        procedureAccessor.setConnection(metaDbConnection);
        procedureAccessor.insertProcedure(procedureSchema, procedureName, createProcedureContent, executionContext);

        PlParameterAccessor plParameterAccessor = new PlParameterAccessor();
        plParameterAccessor.setConnection(metaDbConnection);
        plParameterAccessor.insertProcedureParams(procedureSchema, procedureName, createProcedureContent);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        ProcedureAccessor accessor = new ProcedureAccessor();
        accessor.setConnection(metaDbConnection);
        accessor.dropProcedure(procedureSchema, procedureName);

        PlParameterAccessor plParameterAccessor = new PlParameterAccessor();
        plParameterAccessor.setConnection(metaDbConnection);
        plParameterAccessor.dropProcedureParams(procedureSchema, procedureName);
    }
}
