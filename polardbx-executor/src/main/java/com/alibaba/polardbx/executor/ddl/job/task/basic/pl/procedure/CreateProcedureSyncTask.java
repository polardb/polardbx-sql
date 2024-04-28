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
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.CreateProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.DropProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "CreateProcedureSyncTask")
public class CreateProcedureSyncTask extends BaseDdlTask {

    private String procedureSchema;
    private String procedureName;

    @JSONCreator
    public CreateProcedureSyncTask(String schemaName, String procedureSchema, String procedureName) {
        super(schemaName);
        this.procedureSchema = procedureSchema;
        this.procedureName = procedureName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        updateTaskStateInNewTxn(DdlTaskState.DIRTY);

        FailPoint.injectExceptionFromHint("FP_CREATE_PROCEDURE_ERROR", executionContext);

        SyncManagerHelper.sync(new CreateProcedureSyncAction(procedureSchema, procedureName),
            TddlConstants.INFORMATION_SCHEMA, SyncScope.NOT_COLUMNAR_SLAVE);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        SyncManagerHelper.sync(new DropProcedureSyncAction(procedureSchema, procedureName),
            TddlConstants.INFORMATION_SCHEMA, SyncScope.NOT_COLUMNAR_SLAVE);
    }
}

