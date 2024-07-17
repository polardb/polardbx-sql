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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RenameTablesUpdateDataIdTask")
public class RenameTablesUpdateDataIdTask extends BaseGmsTask {

    private List<String> oldTableNames;
    private List<String> newTableNames;

    @JSONCreator
    public RenameTablesUpdateDataIdTask(String schemaName, List<String> oldTableNames, List<String> newTableNames) {
        super(schemaName, null);
        this.oldTableNames = oldTableNames;
        this.newTableNames = newTableNames;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        for (int i = 0; i < oldTableNames.size(); ++i) {
            String tableName = oldTableNames.get(i);
            String newTableName = newTableNames.get(i);

            TableMetaChanger.renameTableDataId(metaDbConnection, schemaName, tableName, newTableName);
            CommonMetaChanger.renameFinalOperationsOnSuccess(schemaName, tableName, newTableName);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    /**
     * 只改版本，不sync
     */
    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    /**
     * 只改版本，不sync
     */
    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }
}
