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
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "DropTableRemoveMetaTask")
public class DropTableRemoveMetaTask extends BaseGmsTask {

    @JSONCreator
    public DropTableRemoveMetaTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Remove locality of this table
        SchemaManager schemaManager = OptimizerContext.getContext(getSchemaName()).getLatestSchemaManager();
        LocalityManager lm = LocalityManager.getInstance();
        TableMeta tableMeta = schemaManager.getTableWithNull(getLogicalTableName());
        if (tableMeta != null && lm.getLocalityOfTable(tableMeta.getId()) != null) {
            lm.deleteLocalityOfTable(tableMeta.getId());
        }

        // Remove table meta
        TableMetaChanger.removeTableMeta(metaDbConnection, schemaName, logicalTableName, true, executionContext);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        CommonMetaChanger.finalOperationsOnSuccess(schemaName, logicalTableName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterRemovingTableMeta(schemaName, logicalTableName);
    }
}
