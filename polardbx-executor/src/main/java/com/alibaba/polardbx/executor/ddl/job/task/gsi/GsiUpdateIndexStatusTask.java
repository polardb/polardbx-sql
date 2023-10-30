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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * change GSI status
 * <p>
 * will update [indexes]
 * will update [tables (version) ]
 */
@TaskName(name = "GsiUpdateIndexStatusTask")
@Getter
public class GsiUpdateIndexStatusTask extends BaseGmsTask {

    final String indexName;
    final IndexStatus beforeIndexStatus;
    final IndexStatus afterIndexStatus;
    final boolean needOnlineSchemaChange;

    @JSONCreator
    public GsiUpdateIndexStatusTask(String schemaName,
                                    String logicalTableName,
                                    String indexName,
                                    IndexStatus beforeIndexStatus,
                                    IndexStatus afterIndexStatus,
                                    boolean needOnlineSchemaChange) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.beforeIndexStatus = beforeIndexStatus;
        this.afterIndexStatus = afterIndexStatus;
        this.needOnlineSchemaChange = needOnlineSchemaChange;
    }

    /**
     *
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        GsiMetaChanger.updateIndexStatus(
            metaDbConnection, schemaName, logicalTableName, indexName, beforeIndexStatus, afterIndexStatus);

        //in the case of 'drop gsi', cancel ddl is not supported once the JOB has begun
        if (beforeIndexStatus == IndexStatus.PUBLIC && afterIndexStatus == IndexStatus.WRITE_ONLY) {
            updateSupportedCommands(true, false, metaDbConnection);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format("[Job: %d Task: %d] Update GSI table state. schema:%s, table:%s, index:%s, before "
                    + "state:%s, "
                    + "after "
                    + "state:%s",
                getJobId(), getTaskId(),
                schemaName,
                logicalTableName,
                indexName,
                beforeIndexStatus.name(),
                afterIndexStatus.name()));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        GsiMetaChanger.updateIndexStatus(
            metaDbConnection, schemaName, logicalTableName, indexName, afterIndexStatus, beforeIndexStatus);

        //sync have to be successful to continue
        if (needOnlineSchemaChange) {
            SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        LOGGER.info(String
            .format("Rollback Update GSI table state. schema:%s, table:%s, index:%s, before state:%s, after state:%s",
                schemaName,
                logicalTableName,
                indexName,
                beforeIndexStatus.name(),
                afterIndexStatus.name()));
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        if (needOnlineSchemaChange) {
            super.onRollbackSuccess(executionContext);
        }
    }

    @Override
    protected String remark() {
        return String.format("|GSI(%s) %s to %s", indexName, beforeIndexStatus.name(), afterIndexStatus.name());
    }
}
