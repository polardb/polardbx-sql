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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * change GSI column status
 * usually for clustered global secondary index
 * <p>
 * will update [indexes]
 * will update [tables (version) ]
 */
@TaskName(name = "GsiUpdateIndexColumnStatusTask")
@Getter
public class GsiUpdateIndexColumnStatusTask extends BaseGmsTask {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseGmsTask.class);

    final String indexName;
    final List<String> columns;
    final TableStatus beforeTableStatus;
    final TableStatus afterTableStatus;

    @JSONCreator
    public GsiUpdateIndexColumnStatusTask(String schemaName,
                                          String logicalTableName,
                                          String indexName,
                                          List<String> columns,
                                          TableStatus beforeTableStatus,
                                          TableStatus afterTableStatus) {
        super(schemaName, logicalTableName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.columns = columns;
        this.beforeTableStatus = beforeTableStatus;
        this.afterTableStatus = afterTableStatus;
    }

    /**
     * see: DdlGmsUtils#updateAddColumnsStatusWithGsi
     * see: changeAddColumnsStatusWithGsi
     * see: GsiManager#alterGsi# alterTable.dropColumn()
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        GsiMetaChanger.updateIndexColumnStatusMeta(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            columns,
            beforeTableStatus,
            afterTableStatus
        );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format("Update GSI column state. schema:%s, table:%s, index:%s, before state:%s, after state:%s",
                schemaName,
                logicalTableName,
                indexName,
                beforeTableStatus.name(),
                afterTableStatus.name()));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        GsiMetaChanger.updateIndexColumnStatusMeta(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            columns,
            afterTableStatus,
            beforeTableStatus
        );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
        executionContext.refreshTableMeta();

        LOGGER.info(String
            .format("Rollback Update GSI table state. schema:%s, table:%s, index:%s, before state:%s, after state:%s",
                schemaName,
                logicalTableName,
                indexName,
                beforeTableStatus.name(),
                afterTableStatus.name()));
    }

    @Override
    public String remark() {
        return String.format("|GSI column(%s.%s.%s) from %s to %s",
            schemaName,
            logicalTableName,
            indexName,
            beforeTableStatus.name(),
            afterTableStatus.name());
    }
}
