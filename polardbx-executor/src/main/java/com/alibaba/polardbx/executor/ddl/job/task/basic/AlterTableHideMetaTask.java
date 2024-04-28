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
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterTableHideMetaTask")
public class AlterTableHideMetaTask extends BaseGmsTask {

    private List<String> columnNames;
    private List<String> indexNames;

    @JSONCreator
    public AlterTableHideMetaTask(String schemaName, String logicalTableName, List<String> columnNames,
                                  List<String> indexNames) {
        super(schemaName, logicalTableName);
        this.columnNames = columnNames;
        this.indexNames = indexNames;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.hideTableMeta(metaDbConnection, schemaName, logicalTableName, columnNames, indexNames);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.showTableMeta(metaDbConnection, schemaName, logicalTableName, columnNames, indexNames);
        // Refresh table meta to make hidden columns visible after rollback.
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName), SyncScope.ALL);
        executionContext.refreshTableMeta();
    }

}
