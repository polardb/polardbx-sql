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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "ClearAutoPartitionFlagTask")
public class ClearAutoPartitionFlagTask extends BaseGmsTask {

    public ClearAutoPartitionFlagTask(final String schemaName,
                                      final String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        if (!primaryTableMeta.isAutoPartition()) {
            return;
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.setAutoPartitionFlag(metaDbConnection, schemaName, logicalTableName, false);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.setAutoPartitionFlag(metaDbConnection, schemaName, logicalTableName, true);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
        executionContext.refreshTableMeta();
    }

}