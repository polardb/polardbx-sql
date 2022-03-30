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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;

public abstract class BaseGmsTask extends BaseDdlTask {

    protected String logicalTableName;

    public BaseGmsTask(String schemaName, String logicalTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateTableVersion(metaDbConnection);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //this sync invocation may be deleted in the future
        CommonMetaChanger.sync(MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName));
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
        updateTableVersion(metaDbConnection);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //this sync invocation may be deleted in the future
        CommonMetaChanger.sync(MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName));
    }

    /**
     * The real execution flow that a subclass should implement.
     * 只需要实现"最佳实践"中的step 1
     * step 2,3,4由BaseGmsTask管理
     * step 5由BaseSyncTask管理
     */
    protected abstract void executeImpl(Connection metaDbConnection, ExecutionContext executionContext);

    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    protected void updateTableVersion(Connection metaDbConnection) {
        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public String getLogicalTableName() {
        return this.logicalTableName;
    }

}
