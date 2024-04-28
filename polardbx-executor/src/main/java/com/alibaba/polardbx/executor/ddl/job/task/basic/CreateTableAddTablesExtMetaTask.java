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
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateTableAddTablesExtMetaTask")
public class CreateTableAddTablesExtMetaTask extends BaseGmsTask {

    private final boolean autoPartition;
    private boolean temporary;
    private TablesExtRecord tablesExtRecord;

    @JSONCreator
    public CreateTableAddTablesExtMetaTask(String schemaName, String logicalTableName, boolean temporary,
                                           TablesExtRecord tablesExtRecord, boolean autoPartition) {
        super(schemaName, logicalTableName);
        this.autoPartition = autoPartition;
        this.temporary = temporary;
        this.tablesExtRecord = tablesExtRecord;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }
        if (autoPartition) {
            tablesExtRecord.setAutoPartition();
        }
        TableMetaChanger.addTableExt(metaDbConnection, tablesExtRecord);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }
        TableMetaChanger.removeTableExt(metaDbConnection, schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName), SyncScope.ALL);
    }

    private boolean isCreateTableSupported(ExecutionContext executionContext) {
        return !(temporary || executionContext.isUseHint());
    }

    @Override
    public String remark() {
        String partitionStr = "";
        if (this.autoPartition) {
            partitionStr = ",auto-partition by " + this.tablesExtRecord.dbPartitionKey;
        }
        return String.format("|table=%s%s", tablesExtRecord.tableName, partitionStr);
    }
}
