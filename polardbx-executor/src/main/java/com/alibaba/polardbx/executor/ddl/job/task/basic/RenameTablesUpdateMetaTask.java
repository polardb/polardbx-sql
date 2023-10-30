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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.schema.Table;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "RenameTablesUpdateMetaTask")
public class RenameTablesUpdateMetaTask extends BaseGmsTask {

    private List<String> oldTableNames;
    private List<String> newTableNames;

    @JSONCreator
    public RenameTablesUpdateMetaTask(String schemaName, List<String> oldTableNames, List<String> newTableNames) {
        super(schemaName, null);
        this.oldTableNames = oldTableNames;
        this.newTableNames = newTableNames;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        for (int i = 0; i < oldTableNames.size(); ++i) {
            String tableName = oldTableNames.get(i);
            String newTableName = newTableNames.get(i);

            if (isNewPartitionDb) {
                TableMetaChanger
                    .renamePartitionTableMeta(metaDbConnection, schemaName, tableName, newTableName,
                        executionContext, false);
            } else {
                TableMetaChanger
                    .renameTableMeta(metaDbConnection, schemaName, tableName, newTableName, executionContext, false);
            }
            CommonMetaChanger.renameFinalOperationsOnSuccess(schemaName, tableName, newTableName);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        for (int i = oldTableNames.size() - 1; i >= 0; --i) {
            String tableName = newTableNames.get(i);
            String newTableName = oldTableNames.get(i);

            if (isNewPartitionDb) {
                TableMetaChanger
                    .renamePartitionTableMeta(metaDbConnection, schemaName, tableName, newTableName,
                        executionContext);
            } else {
                TableMetaChanger
                    .renameTableMeta(metaDbConnection, schemaName, tableName, newTableName, executionContext);
            }
            CommonMetaChanger.renameFinalOperationsOnSuccess(schemaName, tableName, newTableName);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateTablesVersion(metaDbConnection);
    }

    /**
     * 只改版本，不sync
     */
    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
        updateTablesVersion(metaDbConnection);
    }

    /**
     * 只改版本，不sync
     */
    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    /**
     * 只改版本，不sync
     */
    private void updateTablesVersion(Connection metaDbConnection) {
        if (GeneralUtil.isNotEmpty(newTableNames)) {
            int i = 0;
            long maxVersion = 1;
            try {
                for (String tableName : newTableNames) {
                    long version = TableInfoManager.getTableVersion4Rename(schemaName, tableName, metaDbConnection);
                    maxVersion = Math.max(maxVersion, version);
                }
                for (String tableName : newTableNames) {
                    TableInfoManager.updateTableVersion4Rename(schemaName, tableName, maxVersion + 1, metaDbConnection);
                    i++;
                }
            } catch (Throwable t) {
                LOGGER.error(String.format(
                    "error occurs while update table version, schemaName:%s, tableName:%s", schemaName,
                    newTableNames.get(i)));
                throw GeneralUtil.nestedException(t);
            }
        }
    }
}
