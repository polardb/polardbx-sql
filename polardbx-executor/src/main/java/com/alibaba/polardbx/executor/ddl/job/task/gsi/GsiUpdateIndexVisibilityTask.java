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
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "GsiUpdateIndexVisibilityTask")
public class GsiUpdateIndexVisibilityTask extends BaseGmsTask {

    final String indexName;
    final IndexVisibility beforeVisibility;
    final IndexVisibility afterVisibility;

    @JSONCreator
    public GsiUpdateIndexVisibilityTask(String schemaName,
                                        String logicalTableName,
                                        String indexName,
                                        IndexVisibility beforeVisibility,
                                        IndexVisibility afterVisibility) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.beforeVisibility = beforeVisibility;
        this.afterVisibility = afterVisibility;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        GsiMetaChanger.updateIndexVisibility(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            beforeVisibility,
            afterVisibility
        );

        LOGGER.info(
            String.format(
                "[Job: %d Task: %d] Update GSI table visibility. schema:%s, table:%s, index:%s, from:%s to:%s",
                getJobId(), getTaskId(),
                schemaName,
                logicalTableName,
                indexName,
                beforeVisibility.name(),
                afterVisibility.name()
            )
        );
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        GsiMetaChanger.updateIndexVisibility(
            metaDbConnection,
            schemaName,
            logicalTableName,
            indexName,
            afterVisibility,
            beforeVisibility
        );

        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName), SyncScope.ALL);

        LOGGER.info(
            String.format("Rollback Update GSI table visibility. schema:%s, table:%s, index:%s, from:%s to:%s",
                schemaName,
                logicalTableName,
                indexName,
                afterVisibility.name(),
                beforeVisibility.name()
            )
        );
    }

    @Override
    protected String remark() {
        return String.format("|GSI(%s) change visibility from:%s to:%s", indexName, beforeVisibility.name(),
            afterVisibility.name());
    }

}
