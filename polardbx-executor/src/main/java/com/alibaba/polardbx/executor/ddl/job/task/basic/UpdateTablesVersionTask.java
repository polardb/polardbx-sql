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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Joiner;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "UpdateTablesVersionTask")
public class UpdateTablesVersionTask extends BaseGmsTask {

    final List<String> tableNames;

    public UpdateTablesVersionTask(String schemaName,
                                   List<String> tableNames) {
        super(schemaName, null);
        this.tableNames = tableNames;
    }

    @Override
    protected String remark() {
        return "|updateTablesVersion: " + Joiner.on(", ").join(tableNames);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateTablesVersion(metaDbConnection);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateTablesVersion(metaDbConnection);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    private void updateTablesVersion(Connection metaDbConnection) {
        if (GeneralUtil.isNotEmpty(tableNames)) {
            int i = 0;
            try {
                for (String tableName : tableNames) {
                    TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConnection);
                    i++;
                }
            } catch (Throwable t) {
                LOGGER.error(String.format(
                    "error occurs while update table version, schemaName:%s, tableName:%s", schemaName,
                    tableNames.get(i)));
                throw GeneralUtil.nestedException(t);
            }
        }
    }
}