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
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "RenameGsiUpdateMetaTask")
public class RenameGsiUpdateMetaTask extends RenameTableUpdateMetaTask {

    final private String primaryTableName;

    public RenameGsiUpdateMetaTask(String schemaName, String primaryTableName,
                                   String logicalTableName, String newLogicalTableName) {
        super(schemaName, logicalTableName, newLogicalTableName);
        this.primaryTableName = primaryTableName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        executionContext.setPhyTableRenamed(false);
        super.executeImpl(metaDbConnection, executionContext);

        try {
            TableInfoManager.updateTableVersion(schemaName, primaryTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        executionContext.setPhyTableRenamed(false);
        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartitionDb) {
            TableMetaChanger
                .renamePartitionTableMeta(metaDbConnection, schemaName, newLogicalTableName, logicalTableName,
                    executionContext);
        } else {
            TableMetaChanger
                .renameTableMeta(metaDbConnection, schemaName, newLogicalTableName, logicalTableName, executionContext);
        }

        try {
            TableInfoManager.updateTableVersion(schemaName, primaryTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
        executionContext.refreshTableMeta();
    }
}
