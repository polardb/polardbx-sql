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

package com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn;

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "OnlineModifyColumnAddMetaTask")
public class OnlineModifyColumnAddMetaTask extends BaseGmsTask {
    private final boolean changeColumn;

    private final String newColumnName;
    private final String oldColumnName;
    private final String afterColumnName;

    private final String dbIndex;
    private final String phyTableName;

    private final List<String> coveringGsi;
    private final List<String> gsiDbIndex;
    private final List<String> gsiPhyTableName;

    public OnlineModifyColumnAddMetaTask(String schemaName, String logicalTableName, boolean changeColumn,
                                         String dbIndex, String phyTableName, String newColumnName,
                                         String oldColumnName, String afterColumnName, List<String> coveringGsi,
                                         List<String> gsiDbIndex, List<String> gsiPhyTableName) {
        super(schemaName, logicalTableName);
        this.changeColumn = changeColumn;

        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;

        this.newColumnName = newColumnName;
        this.oldColumnName = oldColumnName;
        this.afterColumnName = afterColumnName;

        this.coveringGsi = coveringGsi;
        this.gsiDbIndex = gsiDbIndex;
        this.gsiPhyTableName = gsiPhyTableName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.onlineModifyColumnAddColumn(metaDbConnection, schemaName, logicalTableName, dbIndex,
            phyTableName, newColumnName, oldColumnName, afterColumnName, coveringGsi, gsiDbIndex, gsiPhyTableName);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.onlineModifyColumnAddColumnRollback(metaDbConnection, schemaName, logicalTableName,
            newColumnName, oldColumnName, coveringGsi);

        for (String gsiName: coveringGsi) {
            ExecutorContext
                .getContext(executionContext.getSchemaName())
                .getGsiManager()
                .getGsiMetaManager()
                .removeColumnMeta(metaDbConnection, schemaName, logicalTableName, gsiName, newColumnName);
        }
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        sb.append("add column ").append(this.newColumnName);
        if (changeColumn) {
            sb.append(" for online change column on table ").append(this.getLogicalTableName());
        } else {
            sb.append(" for online modify column on table ").append(this.getLogicalTableName());
        }
        return "|" + sb;
    }

}
