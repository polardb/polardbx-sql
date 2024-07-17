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

package com.alibaba.polardbx.executor.ddl.job.task.storagepool;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterDatabaseLocalityTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterDatabaseLocalityTask extends BaseDdlTask {

    String schemaName;

    String instId;

    String targetLocality;

    @JSONCreator
    public AlterDatabaseLocalityTask(String schemaName, String instId,
                                     String targetLocality) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.targetLocality = targetLocality;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long dbId = DbInfoManager.getInstance().getDbInfo(schemaName).id;
        LocalityManager.getInstance().setLocalityOfDb(dbId, targetLocality);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
//        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
//        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

}
