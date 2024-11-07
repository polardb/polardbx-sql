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

import java.sql.Connection;

@Getter
@TaskName(name = "RenameTableUpdateMetaTask")
public class RenameTableUpdateMetaTask extends BaseGmsTask {

    protected String newLogicalTableName;
    protected boolean needRenamePhyTables;

    @JSONCreator
    public RenameTableUpdateMetaTask(String schemaName, String logicalTableName, String newLogicalTableName,
                                     boolean needRenamePhyTables) {
        super(schemaName, logicalTableName);
        this.newLogicalTableName = newLogicalTableName;
        this.needRenamePhyTables = needRenamePhyTables;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartitionDb) {
            TableMetaChanger
                .renamePartitionTableMeta(metaDbConnection, schemaName, logicalTableName, newLogicalTableName,
                    needRenamePhyTables, executionContext);

        } else {
            TableMetaChanger
                .renameTableMeta(metaDbConnection, schemaName, logicalTableName, newLogicalTableName,
                    needRenamePhyTables, executionContext);
        }
        TableMetaChanger.renameTtlTableForTtlInfo(metaDbConnection, schemaName, logicalTableName, schemaName,
            newLogicalTableName);
        CommonMetaChanger.renameFinalOperationsOnSuccess(schemaName, logicalTableName, newLogicalTableName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterRenamingTableMeta(schemaName, newLogicalTableName);
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        try {
            TableInfoManager.updateTableVersion(schemaName, newLogicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
