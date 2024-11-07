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
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "TruncateTableRecycleBinTask")
public class TruncateTableRecycleBinTask extends BaseGmsTask {

    private final String binName;
    private final String tmpBinName;

    @JSONCreator
    public TruncateTableRecycleBinTask(String schemaName, String logicalTableName, String binName, String tmpBinName) {
        super(schemaName, logicalTableName);
        this.binName = binName;
        this.tmpBinName = tmpBinName;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.truncateTableWithRecycleBin(schemaName, logicalTableName, binName, tmpBinName,
            metaDbConnection, executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateTablesVersion(metaDbConnection);
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            updateTablesExtVersion(metaDbConnection);
        }
    }

    private void updateTablesVersion(Connection metaDbConnection) {
        try {
            long version = TableInfoManager.getTableVersion4Rename(schemaName, logicalTableName, metaDbConnection);
            long version2 = TableInfoManager.getTableVersion4Rename(schemaName, binName, metaDbConnection);
            long maxVersion = Math.max(version, version2) + 1;
            TableInfoManager.updateTableVersion4Rename(schemaName, logicalTableName, maxVersion, metaDbConnection);
            TableInfoManager.updateTableVersion4Rename(schemaName, binName, maxVersion, metaDbConnection);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while update table version, schemaName:%s, tableName:%s", schemaName, logicalTableName));
            throw GeneralUtil.nestedException(t);
        }
    }

    private void updateTablesExtVersion(Connection metaDbConnection) {
        try {
            long version = TableInfoManager.getTableExtVersion4Rename(schemaName, logicalTableName, metaDbConnection);
            long version2 = TableInfoManager.getTableExtVersion4Rename(schemaName, binName, metaDbConnection);
            long maxVersion = Math.max(version, version2) + 1;
            TableInfoManager.updateTableExtVersion4Rename(schemaName, logicalTableName, maxVersion, metaDbConnection);
            TableInfoManager.updateTableExtVersion4Rename(schemaName, binName, maxVersion, metaDbConnection);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while update tables_ext version, schemaName:%s, tableName:%s", schemaName,
                logicalTableName));
            throw GeneralUtil.nestedException(t);
        }
    }
}
