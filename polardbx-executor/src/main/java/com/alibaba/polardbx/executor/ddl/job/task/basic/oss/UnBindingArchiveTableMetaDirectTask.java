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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "UnBindingArchiveTableMetaDirectTask")
public class UnBindingArchiveTableMetaDirectTask extends BaseGmsTask {
    private String archiveSchemaName;
    private String archiveTableName;
    private boolean useRowLevelTtl = false;

    @JSONCreator
    public UnBindingArchiveTableMetaDirectTask(String schemaName,
                                               String logicalTableName,
                                               boolean useRowLevelTtl,
                                               String archiveSchemaName,
                                               String archiveTableName) {
        super(schemaName, logicalTableName);
        this.archiveSchemaName = archiveSchemaName;
        this.archiveTableName = archiveTableName;
        this.useRowLevelTtl = useRowLevelTtl;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        if (useRowLevelTtl) {
            tableInfoManager
                .unBindingByArchiveTableNameForTtlInfo(getSchemaName(), archiveTableName);
        } else {
            tableInfoManager
                .unBindingByArchiveTableName(getSchemaName(), archiveTableName);
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager
            .updateArchiveTable(schemaName, logicalTableName, getSchemaName(), archiveTableName);
    }

    protected String remark() {
        return "|remove binding " + schemaName + "." + logicalTableName
            + " to " + archiveSchemaName + "." + archiveTableName;
    }
}
