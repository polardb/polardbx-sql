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
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * see DropTableRemoveMetaTask
 */
@Getter
@TaskName(name = "DropGsiTableRemoveMetaTask")
public class DropGsiTableRemoveMetaTask extends BaseGmsTask {

    protected final String indexName;

    @JSONCreator
    public DropGsiTableRemoveMetaTask(String schemaName, String logicalTableName, String indexName) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        LOGGER.info(String.format("[Job: %d Task: %d] Remove Gsi Meta for [table=%s,index=%s]",
            this.getJobId(), this.getTaskId(), logicalTableName, indexName));
        TableMetaChanger.removeTableMeta(metaDbConnection, schemaName, indexName, true, executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        CommonMetaChanger.dropTableFinalOperationsOnSuccess(schemaName, indexName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterRemovingTableMeta(schemaName, logicalTableName);
    }
}
