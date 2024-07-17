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
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * change CCI status
 * <p>
 * will update [indexes]
 * will update [tables (version) ]
 */
@TaskName(name = "CciUpdateIndexStatusTask")
@Getter
public class CciUpdateIndexStatusTask extends BaseGmsTask {

    final String indexName;
    final ColumnarTableStatus beforeColumnarTableStatus;
    final ColumnarTableStatus afterColumnarTableStatus;
    final IndexStatus beforeIndexStatus;
    final IndexStatus afterIndexStatus;
    final boolean needOnlineSchemaChange;

    @JSONCreator
    public CciUpdateIndexStatusTask(String schemaName,
                                    String logicalTableName,
                                    String indexName,
                                    ColumnarTableStatus beforeColumnarTableStatus,
                                    ColumnarTableStatus afterColumnarTableStatus,
                                    IndexStatus beforeIndexStatus,
                                    IndexStatus afterIndexStatus,
                                    boolean needOnlineSchemaChange) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.beforeColumnarTableStatus = beforeColumnarTableStatus;
        this.afterColumnarTableStatus = afterColumnarTableStatus;
        this.beforeIndexStatus = beforeIndexStatus;
        this.afterIndexStatus = afterIndexStatus;
        this.needOnlineSchemaChange = needOnlineSchemaChange;
    }

    /**
     *
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Update columnar table status.
        if (beforeColumnarTableStatus != afterColumnarTableStatus) {
            TableInfoManager.updateColumnarTableStatus(
                metaDbConnection,
                schemaName,
                logicalTableName,
                indexName,
                beforeColumnarTableStatus,
                afterColumnarTableStatus);
        }

        // Update columnar index status.
        TableInfoManager.updateIndexStatus(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            afterIndexStatus
        );

        //in the case of 'drop cci', cancel ddl is not supported once the JOB has begun
        boolean dropCci = (beforeColumnarTableStatus == ColumnarTableStatus.PUBLIC)
            && (afterColumnarTableStatus == ColumnarTableStatus.ABSENT
            || afterColumnarTableStatus == ColumnarTableStatus.DROP);
        if (dropCci) {
            updateSupportedCommands(true, false, metaDbConnection);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format("[Job: %d Task: %d] Update CCI state. "
                    + "schema:%s, table:%s, index:%s, before columnar table state:%s, after columnar table state:%s, "
                    + "before index state:%s, after index state:%s",
                getJobId(),
                getTaskId(),
                schemaName,
                logicalTableName,
                indexName,
                beforeColumnarTableStatus.name(),
                afterColumnarTableStatus.name(),
                beforeIndexStatus.name(),
                afterIndexStatus.name()));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (beforeColumnarTableStatus != afterColumnarTableStatus) {
            TableInfoManager.updateColumnarTableStatus(
                metaDbConnection,
                schemaName,
                logicalTableName,
                indexName,
                afterColumnarTableStatus,
                beforeColumnarTableStatus);
        }

        TableInfoManager.updateIndexStatus(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            beforeIndexStatus
        );

        //sync have to be successful to continue
        if (needOnlineSchemaChange) {
            SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName), SyncScope.ALL);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format("[Job: %d Task: %d] Rollback Update CCI table state. "
                    + "schema:%s, table:%s, index:%s, before columnar table state:%s, after columnar table state:%s, "
                    + "before index state:%s, after index state:%s",
                getJobId(),
                getTaskId(),
                schemaName,
                logicalTableName,
                indexName,
                beforeColumnarTableStatus.name(),
                afterColumnarTableStatus.name(),
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
        return String.format("|CCI(%s) %s to %s", indexName, beforeColumnarTableStatus.name(),
            afterColumnarTableStatus.name());
    }
}
